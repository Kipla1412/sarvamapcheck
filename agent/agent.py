from __future__ import annotations
from typing import AsyncGenerator, Awaitable, Callable
from agent.events import AgentEvent, AgentEventType
from agent.session import Session
from client.response import StreamEventType, TokenUsage, ToolCall, ToolResultMessage, parse_tool_call_arguments
from config.config import Config
from prompts.system import create_loop_breaker_prompt
from tools.base import ToolConfirmation
import json
import time
from datetime import datetime
import asyncio
from speechtospeech.webvad import VoiceActivityDetector

class Agent:
    def __init__(
        self,
        config: Config,
        confirmation_callback: Callable[[ToolConfirmation], bool] | None = None,
    ):
        self.config = config
        self.session: Session | None = Session(self.config)
        
        if confirmation_callback is not None:
            self.session.approval_manager.confirmation_callback = confirmation_callback

    async def run(self, message: str):

        self.session.start_mlflow_run(message)
        
        await self.session.hook_system.trigger_before_agent(message)

        yield AgentEvent.agent_start(message)
        
        self.session.context_manager.add_user_message(message)

        final_response: str | None = None

        async for event in self._agentic_loop():
            yield event

            if event.type == AgentEventType.TEXT_COMPLETE:
                final_response = event.data.get("content")

        await self.session.hook_system.trigger_after_agent(message, final_response)

        # Extract tools used and token usage from the session
        tools_used = []  # You can populate this from the session context
        token_usage = None  # You can extract this from the LLM client response
        
        yield AgentEvent.agent_end(final_response)

        self.session.end_mlflow_run()

    async def _agentic_loop(self) -> AsyncGenerator[AgentEvent, None]:
        max_turns = self.config.max_turns

        for turn_num in range(max_turns):
            self.session.increment_turn()
            
            while not self.session.event_queue.empty():

                event = await self.session.event_queue.get()

                data = event["data"]
                
                print("DEBUG: agent emitting approval event", data)

                yield AgentEvent.approval_request(
                    approval_id=data["approval_id"],
                    tool_name=data["tool_name"],
                    description=data["description"],
                    params=data.get("params")
                )
            
            response_text = ""

            # check for context overflow
            if self.session.context_manager.needs_compression():
                summary, usage = await self.session.chat_compactor.compress(
                    self.session.context_manager
                )

                if summary:
                    self.session.context_manager.replace_with_summary(summary)
                    self.session.context_manager.set_latest_usage(usage)
                    self.session.context_manager.add_usage(usage)

            tool_schemas = self.session.tool_registry.get_schemas()

            tool_calls: list[ToolCall] = []
            usage: TokenUsage | None = None

            async for event in self.session.client.chat_completion(
                self.session.context_manager.get_messages(),
                tools=tool_schemas if tool_schemas else None,
            ):
                if event.type == StreamEventType.TEXT_DELTA:
                    if event.text_delta:
                        content = event.text_delta.content
                        response_text += content
                        yield AgentEvent.text_delta(content)
                elif event.type == StreamEventType.TOOL_CALL_COMPLETE:
                    if event.tool_call:
                        tool_calls.append(event.tool_call)
                elif event.type == StreamEventType.ERROR:
                    yield AgentEvent.agent_error(
                        event.error or "Unknown error occurred.",
                    )
                elif event.type == StreamEventType.MESSAGE_COMPLETE:
                    usage = event.usage

                    if usage:
                        self.session.mlflow_tracker.log_metrics({
                            "prompt_tokens": usage.prompt_tokens,
                            "completion_tokens": usage.completion_tokens,
                            "total_tokens": usage.total_tokens
                        })

            self.session.context_manager.add_assistant_message(
                response_text or "",
                (
                    [
                        {
                            "id": tc.call_id,
                            "type": "function",
                            "function": {
                                "name": tc.name,
                                "arguments": json.dumps(tc.arguments),
                            },
                        }
                        for tc in tool_calls
                    ]
                    if tool_calls
                    else None
                ),
            )
            
            if response_text:
                yield AgentEvent.text_complete(response_text)
                self.session.loop_detector.record_action(
                    "response",
                    text=response_text,
                )

            if not tool_calls:
                if usage:
                    self.session.context_manager.set_latest_usage(usage)
                    self.session.context_manager.add_usage(usage)

                self.session.context_manager.prune_tool_outputs()
                return

            tool_call_results: list[ToolResultMessage] = []

            for tool_call in tool_calls:
                yield AgentEvent.tool_call_start(
                    tool_call.call_id,
                    tool_call.name,
                    tool_call.arguments,
                )

                self.session.loop_detector.record_action(
                    "tool_call",
                    tool_name=tool_call.name,
                    args=tool_call.arguments,
                )
                # parsed_args = parse_tool_call_arguments(tool_call.arguments)
                
                print(f"DEBUG: Tool {tool_call.name} called with args: {tool_call.arguments}")
                
                with self.session.mlflow_tracker.start_span(
                    name=tool_call.name,
                    attributes={
                        "span_type": "tool",
                        "tool.name": tool_call.name,
                        "tool.args": json.dumps(tool_call.arguments)[:500],
                    },
                ):
                    # result = await self.session.tool_registry.invoke(
                    #     tool_call.name,
                    #     tool_call.arguments,
                    #     self.config.cwd,
                    #     self.session.hook_system,
                    #     self.session.approval_manager,
                    # )

                    # Wrap the tool invocation in a task so it runs in the background
                    invocation_task = asyncio.create_task(
                        self.session.tool_registry.invoke(
                            tool_call.name,
                            tool_call.arguments,
                            self.config.cwd,
                            self.session.hook_system,
                            self.session.approval_manager,
                        )
                    )

                    # While the tool is running (it might be paused waiting for approval!)
                    # We "drain" the event queue and yield requests to the frontend
                    result = None
                    while not invocation_task.done():
                        try:
                            # Check queue every 0.1s. If an approval is added, yield it immediately.
                            event_data = await asyncio.wait_for(self.session.event_queue.get(), timeout=0.1)
                            data = event_data["data"]
                            yield AgentEvent.approval_request(
                                approval_id=data["approval_id"],
                                tool_name=data["tool_name"],
                                description=data["description"],
                                params=data.get("params")
                            )
                        except asyncio.TimeoutError:
                            continue 
                    
                    result = await invocation_task
                    

                    yield AgentEvent.tool_call_complete(
                        tool_call.call_id,
                        tool_call.name,
                        result,
                    )

                    tool_call_results.append(
                        ToolResultMessage(
                            tool_call_id=tool_call.call_id,
                            content=result.to_model_output(),
                            is_error=not result.success,
                        )
                    )

            for tool_result in tool_call_results:
                self.session.context_manager.add_tool_result(
                    tool_result.tool_call_id,
                    tool_result.content,
                )

            loop_detection_error = self.session.loop_detector.check_for_loop()
            if loop_detection_error:
                loop_prompt = create_loop_breaker_prompt(loop_detection_error)
                self.session.context_manager.add_user_message(loop_prompt)

            if usage:
                self.session.context_manager.set_latest_usage(usage)
                self.session.context_manager.add_usage(usage)

            self.session.context_manager.prune_tool_outputs()
        yield AgentEvent.agent_error(f"Maximum turns ({max_turns}) reached")

    async def run_audio(self, audio, rate, session: Session | None = None):
        import io
        import soundfile as sf
        import numpy as np
        
        active_session = session or self.session

        if not hasattr(self, "vad"):
            
            self.vad = VoiceActivityDetector()

        # Add new audio chunk to buffer
        active_session.audio_buffer.append(audio)

        # Skip processing if we're already processing this utterance
        if hasattr(active_session, 'is_processing') and active_session.is_processing:
            print("DEBUG: Already processing, skipping audio chunk")
            return

        # Add cooldown after processing to prevent immediate re-processing
        if hasattr(active_session, 'last_process_time'):
            import time
            if time.time() - active_session.last_process_time < 2.0:  # 2 second cooldown
                print("DEBUG: In cooldown period, skipping")
                return

        # Speech detection + barge-in handling
        if self.vad.is_speech(audio):

            # User interrupts while agent speaking
            if active_session.agent_speaking:
                print("DEBUG: Barge-in detected")

                # stop current TTS
                active_session.agent_speaking = False

                # reset audio buffer
                active_session.reset_audio_buffer()

            active_session.silence_chunks = 0

        else:
            active_session.silence_chunks += 1
        
        # Process if we have speech followed by silence (user finished speaking)
        if len(active_session.audio_buffer) > 10 and active_session.silence_chunks >= 4:  # Faster response while preventing multiple processing
            import time
            timestamp = time.strftime("%H:%M:%S", time.localtime())
            print(f"DEBUG: [{timestamp}] User finished speaking, processing...")
            print(f"DEBUG: Buffer size: {len(active_session.audio_buffer)}, Silence chunks: {active_session.silence_chunks}")
            
            # Set processing flag to prevent multiple processing
            active_session.is_processing = True
            active_session.last_process_time = time.time()
            
            # Combine all buffered audio
            total_audio = b''.join(active_session.audio_buffer)
            
            # Reset buffers
            active_session.reset_audio_buffer()
            
            # Only process if we have enough audio (at least 0.3 seconds)
            min_bytes = int(16000 * 0.3 * 2)
            if len(total_audio) < min_bytes:
                print("DEBUG: Too short, ignoring")
                active_session.is_processing = False
                return
        else:
            # Keep collecting audio
            return

        # 1. STT Setup
        stt_engine = self.config.stt_engine
        
        try:
            # Convert raw bytes to Numpy Array
            raw_audio_array = (
                np.frombuffer(total_audio, dtype=np.int16)
                .astype(np.float32) / 32768.0
            )

            samplerate = 16000
            # Now the processor can handle it because it's an array with .ndim
            processed = stt_engine.processor.process(raw_audio_array, samplerate)
            audio_bytes = stt_engine.processor.to_bytes(processed)

            # 2. Transcribe (using to_thread because it's a blocking network call)
            user_text = await asyncio.to_thread(
                stt_engine.provider.transcribe,
                audio_bytes
            )
            
            # DEBUG: Log what STT returned
            import time
            timestamp = time.strftime("%H:%M:%S", time.localtime())
            print(f"DEBUG: [{timestamp}] STT returned: '{user_text}' (length: {len(user_text) if user_text else 0})")
            print(f"DEBUG: Audio processed: {len(total_audio)} bytes, duration: {len(total_audio)/(16000*2):.2f}s")
            
        except Exception as e:
            print(f"DEBUG: STT Exception: {str(e)}")
            yield AgentEvent.agent_error(f"STT/Processing Error: {str(e)}")
            active_session.is_processing = False
            return

        # Handle empty speech
        if not user_text or not user_text.strip():
            print("DEBUG: No speech detected or empty transcription")
            yield AgentEvent.agent_error("No speech detected - please speak clearly")
            active_session.is_processing = False
            return

        # Filter out very short transcriptions (likely noise)
        clean_text = user_text.strip().lower()

        if len(clean_text) < 3:
            print("DEBUG: Transcription too short, likely noise")
            active_session.is_processing = False
            return

        # Send user question to frontend
        yield AgentEvent.user_question(user_text.strip())

        # 3. Run normal agent loop
        import time
        llm_start = time.time()
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        print(f"DEBUG: [{timestamp}] Starting LLM processing for: '{user_text.strip()}'")
        
        async for event in self.run(user_text):
            yield event

            # 4. Convert final response → speech
            if event.type == AgentEventType.TEXT_COMPLETE:
                try:
                    llm_end = time.time()
                    timestamp = time.strftime("%H:%M:%S", time.localtime())
                    print(f"DEBUG: [{timestamp}] LLM completed in {llm_end-llm_start:.2f}s")
                    print(f"DEBUG: Response: '{event.data['content'][:100]}...'")
                    
                    tts = self.config.tts_engine
                    
                    # Wrap synthesize in to_thread so it doesn't block the async loop
                    tts_start = time.time()
                    audio_out, sr = await asyncio.to_thread(
                        tts.synthesize, 
                        event.data["content"]

                    )
                    tts_end = time.time()
                    timestamp = time.strftime("%H:%M:%S", time.localtime())
                    print(f"DEBUG: [{timestamp}] TTS completed in {tts_end-tts_start:.2f}s")

                    active_session.agent_speaking = True

                    audio_bytes = tts.processor.array_to_wav_bytes(audio_out, sr)
                    
                    yield AgentEvent.voice_output(audio_bytes, sr)

                    active_session.agent_speaking = False

                except Exception as e:
                    yield AgentEvent.agent_error(f"TTS Error: {str(e)}")
        
        # Clear processing flag when done
        active_session.is_processing = False
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        print(f"DEBUG: [{timestamp}] Processing cycle completed")
        
    async def __aenter__(self) -> Agent:
        
        await self.session.initialize()
        for tool in self.session.tool_registry.get_tools():
            # Check if the tool class has a 'session' attribute
            if hasattr(tool, 'session'):
                tool.session = self.session
                print(f"DEBUG: Linked Session to Tool: {tool.name}")
        return self

    async def __aexit__(
        self,
        exc_type,
        exc_val,
        exc_tb,  
    ) -> None:
        if self.session and self.session.client:
            await self.session.client.close()
            # Cleanup MLflow run
            self.session.cleanup()
            self.session = None