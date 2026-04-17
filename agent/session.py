from datetime import datetime
import json
from typing import Any
import uuid
from contextlib import asynccontextmanager
from client.llm_client import LLMClient
from config.config import Config
from config.loader import get_data_dir
from context.compaction import ChatCompactor
from context.loop_detector import LoopDetector
from context.manager import ContextManager
from hooks.hook_system import HookSystem
from safety.approval import ApprovalManager
from tools.discovery import ToolDiscoveryManager
from tools.registry import create_default_registry
from utils.mlflow_tracker import get_mlflow_tracker
from typing import List, Dict, Any, Optional
import asyncio
import mlflow


class Session:
    def __init__(self, config: Config):

        self.config = config
        self.client = LLMClient(config=config)
        self.tool_registry = create_default_registry(config=config)
        self.context_manager: ContextManager | None = None
        self.discovery_manager = ToolDiscoveryManager(
            self.config,
            self.tool_registry,
        )
        self.chat_compactor = ChatCompactor(self.client)
      
        self.pending_approvals = {}
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.approval_manager = ApprovalManager(
            self.config.approval,
            self.config.cwd,
            confirmation_callback=self._request_user_confirmation,
        )

        # Add global MLflow tracker
        self.mlflow_tracker = get_mlflow_tracker()
        self.mlflow_run = None
        self.loop_detector = LoopDetector()
        self.hook_system = HookSystem(self.config)
        self.session_id = str(uuid.uuid4())
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.turn_count = 0

        self.audio_buffer: list[bytes] = []
        self.silence_chunks: int = 0
        self.agent_speaking = False
        self.speech_frames = 0
        self.non_speech_frames = 0

    async def _request_user_confirmation(self, confirmation):

        print("DEBUG: _request_user_confirmation START")
        approval_id = str(uuid.uuid4())

        future = asyncio.get_running_loop().create_future()

        self.pending_approvals[approval_id] = future
        print("WAITING APPROVAL:", approval_id)

        # send approval event to frontend
        await self.event_queue.put({
            "type": "approval_request",
            "data": {
                "approval_id": approval_id,
                "tool_name": confirmation.tool_name,
                "description": confirmation.description,
                "params": confirmation.params
            }
        })

        try:
            # Code stops here until the /approve route calls future.set_result()
            approved = await future
            print(f"DEBUG: Approval result received for {approval_id}: {approved}")
            return approved
        finally:
            # Ensure we ALWAYS clean up the dictionary, even if there's an error
            if approval_id in self.pending_approvals:
                del self.pending_approvals[approval_id]

    async def handle_approval(self, approval_id: str, approved: bool) -> bool:
        """
        This is the 'Resolver'. It is called by the WebSocket or API 
        when the user actually clicks 'Approve' or 'Deny'.
        """
        if approval_id in self.pending_approvals:
            future = self.pending_approvals[approval_id]
            if not future.done():
                # This 'wakes up' the _request_user_confirmation method above
                future.set_result(approved)
                return True
        
        print(f"DEBUG: Could not find pending approval for ID: {approval_id}")
        return False

    @property
    def turn_count(self) -> int:
        return getattr(self, '_turn_count', 0)
    
    @turn_count.setter
    def turn_count(self, value: int) -> None:
        self._turn_count = value

    async def initialize(self) -> None:
        
        self.discovery_manager.discover_all()
        self.context_manager = ContextManager(
            config=self.config,
            user_memory=self._load_memory(),
            tools=self.tool_registry.get_tools(),
        )
        
        # Setup MLflow run for this session (optional)
        try:
            if mlflow.active_run():
                mlflow.end_run()
            self.mlflow_run = self.mlflow_tracker.start_run("initializing")
        except Exception as e:
            print(f"Warning: MLflow tracking disabled: {e}")
            self.mlflow_run = None
       
        # Set session ID for trace tracking
        if self.mlflow_tracker:
            self.mlflow_tracker.current_session_id = self.session_id

    def _load_memory(self) -> str | None:
        data_dir = get_data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        path = data_dir / "user_memory.json"

        if not path.exists():
            return None

        try:
            content = path.read_text(encoding="utf-8")
            data = json.loads(content)
            entries = data.get("entries")
            if not entries:
                return None

            lines = ["User preferences and notes:"]
            for key, value in entries.items():
                lines.append(f"- {key}: {value}")

            return "\n".join(lines)
        except Exception:
            return None

    def increment_turn(self) -> int:
        self.turn_count += 1
        self.updated_at = datetime.now()

        return self.turn_count

    def get_stats(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "turn_count": self.turn_count,
            "message_count": self.context_manager.message_count,
            "token_usage": self.context_manager.total_usage,
            "tools_count": len(self.tool_registry.get_tools()),
            # "mlflow_stats": self.mlflow_tracker.get_session_stats()
        }


    def reset_audio_buffer(self):
        self.audio_buffer = []
        self.silence_chunks = 0
        

    @asynccontextmanager
    async def trace_agent_run(self, user_message: str):

        with self.mlflow_tracker.start_span(
            name="agent_run",
            attributes={
                "span_type": "agent",
                "session_id": self.session_id,
                "user_message": user_message[:200],
            },
        ):
            yield

    def start_mlflow_run(self, message: str):
        
        try:
            self.mlflow_run = self.mlflow_tracker.start_run(
                run_name=f"agent-session-{self.session_id}"
            )
            
            self.mlflow_tracker.set_tag("session_id", self.session_id)
            self.mlflow_tracker.set_tag("user_message", message[:200])
        except Exception as e:
            print(f"Warning: Failed to start MLflow run: {e}")
            self.mlflow_run = None


    def end_mlflow_run(self):

        if self.mlflow_run:
            self.mlflow_tracker.end_run()
            self.mlflow_run = None

    def cleanup(self):
        """Cleanup session resources."""
        self.end_mlflow_run()
