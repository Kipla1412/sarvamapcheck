from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import base64
import json
import logging
from agent.events import AgentEventType
from agent.session import Session
from agent.agent import Agent
from api.auth import decode_token
import time


logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/audio")
async def audio_stream(ws: WebSocket):

    # AUTH (your existing logic)
    token = None

    auth_header = ws.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]

    if not token:
        token = ws.query_params.get("token")

    if not token:
        await ws.close(code=1008)
        return

    try:
        user = decode_token(token)
        ws.state.user = user
    except Exception:
        await ws.close(code=1008)
        return

    await ws.accept()

    user_id = str(user.get("id"))

    print(f"🎤 Connected user: {user_id}")

    agent: Agent = ws.app.state.agent
    sessions = ws.app.state.sessions

    # CREATE / GET USER SESSION
    if user_id not in sessions:
        sessions[user_id] = Session(ws.app.state.config)
        await sessions[user_id].initialize()

    session = sessions[user_id]

    try:
        while True:
            audio_chunk = await ws.receive_bytes()
            session.last_activity = time.time()  # ADD THIS

            # IMPORTANT: pass session
            result = await agent.run_audio(
                audio_chunk,
                rate=16000,
                session=session
            )

            if result:
                await ws.send_json({
                    "type": "final",
                    "text": result
                })

    except WebSocketDisconnect:
        print(f"{user_id} disconnected")

        # CLEANUP (important)
        if user_id in sessions:
            if sessions[user_id].client:
                await sessions[user_id].client.close()
            del sessions[user_id]

    except Exception as e:
        print("Runtime error:", e)
        await ws.send_json({
            "type": "error",
            "message": str(e)
        })

