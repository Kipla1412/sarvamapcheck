from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
import asyncio
import logging
import base64
import time
from agent.agent import Agent
from agent.session import Session
from agent.voiceagent import VoiceSession
from speechtospeech.providers.stt.streamsarvam import SarvamStreamingSTTProvider
from speechtospeech.providers.tts.streamsarvam import SarvamStreamingTTSProvider

from api.auth import decode_token

logger = logging.getLogger(__name__)
router = APIRouter()

@router.websocket("/audio")
async def audio_stream(ws: WebSocket):
    token = None

    # 1. Parse authentication context headers/query params
    auth_header = ws.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]

    if not token:
        token = ws.query_params.get("token")

    if not token:
        logger.warning("WebSocket handshake rejected: Missing authentication token.")
        await ws.close(code=1008)
        return

    # 2. Authenticate the Token (Local bypass parameters injected for verification)
    try:
        user = decode_token(token, options={"verify_exp": False})
        ws.state.user = user
    except Exception as e:
        logger.error(f"WebSocket Auth Failed: {e}")
        await ws.close(code=1008)
        return
    
    await ws.accept()
    print("Voice Session Started")
    ws_write_lock = asyncio.Lock()

    # Helper function for safe writing
    async def safe_send_json(data):
        async with ws_write_lock:
            await ws.send_json(data)

    user_id = str(user.get("id", "unknown_user"))
    agent: Agent = ws.app.state.agent
    config = ws.app.state.config
    sessions = ws.app.state.sessions

    # 3. Retrieve or Construct Structural Persistent Session Context
    if user_id not in sessions:
        sessions[user_id] = Session(config)
        await sessions[user_id].initialize()

    session = sessions[user_id]

    # Initialize streaming resource interfaces
    stt = SarvamStreamingSTTProvider(
        api_key=config.sarvam_api_key,
        model=config.sarvam_stt_model
    )

    tts = SarvamStreamingTTSProvider(
        api_key=config.sarvam_api_key,
        model=config.sarvam_tts_model,
        speaker=config.sarvam_speaker
    )

    voice_session = VoiceSession(agent=agent, tts=tts)

    print("Pre-warming streaming providers...")
    await asyncio.gather(
        stt.connect(),
        tts.connect()
    )

    transcript_queue = asyncio.Queue()
    active_response_task = None

    try:
        async def receive_audio():
            """Inbound Client Microphone Stream Loop"""
            try:
                while True:
                    audio_chunk = await ws.receive_bytes()
                    # Keep session active state alive on each incoming audio chunk packet
                    session.last_activity = time.time()
                    await stt.send_audio(audio_chunk)
            except WebSocketDisconnect:
                raise
            except Exception as e:
                logger.error(f"Error reading client audio bytes: {e}")
            
        async def generate_and_send_response(transcript_text: str):
            """Outbound Assistant Streaming Loop"""
            try:
                # Ippo ithu {'type': 'text', ...} or {'type': 'audio', ...} nu yield pannum
                async for response in voice_session.process_transcript_to_audio(transcript_text):
                    if not response:
                        continue

                    # 1. Handle Text
                    if response.get("type") == "text":
                        await safe_send_json({
                            "type": "text",
                            "text": response.get("content")
                        })
                    
                    # 2. Handle Audio
                    elif response.get("type") == "audio":
                        audio_bytes = response.get("content")
                        if isinstance(audio_bytes, bytes):
                            audio_payload = base64.b64encode(audio_bytes).decode("utf-8")
                            await safe_send_json({
                                "type": "audio",
                                "audio": audio_payload
                            })

            except asyncio.CancelledError:
                logger.info("Response generation task explicitly cancelled.")
            except Exception as e:
                if "1000" in str(e) or "408" in str(e):
                    print("TTS connection dropped. Resetting socket.")
                    tts.socket = None
                logger.error(f"Error in response generation: {e}")
        # async def generate_and_send_response(transcript_text: str):
        #     """Outbound Assistant Audio Generation Loop"""
        #     try:
        #         # pass session reference context if needed by internal audio tools down the line
        #         async for audio_chunk in voice_session.process_transcript_to_audio(transcript_text):
        #             if not audio_chunk:
        #                 continue

        #             if isinstance(audio_chunk, bytes):
        #                 audio_payload = base64.b64encode(audio_chunk).decode("utf-8")
        #             else:
        #                 audio_payload = audio_chunk

        #             if isinstance(audio_payload, str) and audio_payload.strip():
        #                 await ws.send_json({
        #                     "type": "audio",
        #                     "audio": audio_payload
        #                 })
        #     except asyncio.CancelledError:
        #         logger.info("Response generation task explicitly cancelled.")
        #     except Exception as e:
        #         if "1000" in str(e) or "408" in str(e):
        #             print("TTS connection dropped mid-stream. Resetting socket object reference.")
        #             tts.socket = None
        #         logger.error(f"Error in response generation execution: {e}")

        async def process_transcripts():
            """Inbound STT Stream Reader Loop"""
            async for transcript_data in stt.stream_transcripts():
                transcript = transcript_data["text"].strip()
                if not transcript:
                    continue

                print(f"USER ({user_id}): {transcript}")
                await ws.send_json({"type": "transcript", "text": transcript})
                
                # Push phrase data safely down the pipeline execution queue
                await transcript_queue.put(transcript)

        async def dispatch_responses():
            """Dedicated Queue Consumer Task to handle voice interrupts clean"""
            nonlocal active_response_task
            while True:
                try:
                    transcript = await transcript_queue.get()
                    
                    if active_response_task and not active_response_task.done():
                        print("Interrupting active speaking block for new phrase...")
                        active_response_task.cancel()
                        try:
                            await active_response_task
                        except asyncio.CancelledError:
                            pass
                        try:
                            await tts.flush()
                        except Exception:
                            pass

                    active_response_task = asyncio.create_task(generate_and_send_response(transcript))
                    await active_response_task
                    transcript_queue.task_done()
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Queue Orchestration failure context: {e}")

        # Initialize simultaneous task execution workers
        receive_task = asyncio.create_task(receive_audio())
        transcript_task = asyncio.create_task(process_transcripts())
        dispatch_task = asyncio.create_task(dispatch_responses())

        done, pending = await asyncio.wait(
            [receive_task, transcript_task, dispatch_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            task.result()

    except WebSocketDisconnect:
        print(f"Client {user_id} disconnected gracefully.")
    except Exception as e:
        logger.exception(e)
        try:
            await ws.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        # Tear down ongoing operations cleanly without locking resources open
        if active_response_task and not active_response_task.done():
            active_response_task.cancel()
        
        for task_var in ['receive_task', 'transcript_task', 'dispatch_task']:
            if task_var in locals():
                task_obj = locals()[task_var]
                if not task_obj.done():
                    task_obj.cancel()

        try:
            await stt.close()
            await tts.close()
        except Exception:
            pass
        
        # Safe cleanup block layout - prevents immediate session wipe on small connection losses
        if user_id in sessions:
            if sessions[user_id].client:
                await sessions[user_id].client.close()
            del sessions[user_id]
            
        print("Voice Session Closed Safely")

# from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, status
# import asyncio
# import logging
# import base64
# import jwt
# from agent.agent import Agent
# from agent.voiceagent import VoiceSession
# from speechtospeech.providers.stt.streamsarvam import SarvamStreamingSTTProvider
# from speechtospeech.providers.tts.streamsarvam import SarvamStreamingTTSProvider

# from api.auth import decode_token

# logger = logging.getLogger(__name__)
# router = APIRouter()

# @router.websocket("/audio")
# async def audio_stream(ws: WebSocket):

#     token = None

#     auth_header = ws.headers.get("Authorization")
#     if auth_header and auth_header.startswith("Bearer "):
#         token = auth_header.split(" ")[1]

#     if not token:
#         token = ws.query_params.get("token")

#     if not token:
#         logger.warning("WebSocket handshake rejected: Missing authentication token.")
#         await ws.close(code=1008)
#         return

#     try:
#         user = decode_token(token)
#         ws.state.user = user
#     except Exception:
#         await ws.close(code=1008)
#         return
#     # if not token:
#     #     logger.warning("WebSocket handshake rejected: Missing query token.")
#     #     await ws.close(code=status.WS_1008_POLICY_VIOLATION)
#     #     return

#     # try:
#     #     # Authenticate using your actual JWKS decode framework
#     #     user_payload = decode_token(token)
        
#     #     # Verify specific resource permission if required by your pipeline
#     #     permissions = user_payload.get("permissions", [])
#     #     if "patient:read" not in permissions and "intake:write" not in permissions:
#     #         # Drop connection if user lacks valid scope
#     #         logger.warning("WebSocket handshake rejected: Insufficient scope permissions.")
#     #         await ws.close(code=status.WS_1008_POLICY_VIOLATION)
#     #         return

#     # except jwt.ExpiredSignatureError:
#     #     logger.warning("WebSocket Auth Failed: Token expired.")
#     #     await ws.close(code=status.WS_1008_POLICY_VIOLATION)
#     #     return
#     # except jwt.InvalidTokenError:
#     #     logger.warning("WebSocket Auth Failed: Invalid token structure.")
#     #     await ws.close(code=status.WS_1008_POLICY_VIOLATION)
#     #     return
#     # except Exception as e:
#     #     logger.error(f"WebSocket Auth Failed: Unexpected error: {e}")
#     #     await ws.close(code=status.WS_1011_INTERNAL_ERROR)
#     #     return
    
#     await ws.accept()
#     print("Voice Session Started")

#     agent: Agent = ws.app.state.agent
#     config = ws.app.state.config
#     sessions = ws.app.state.sessions


#     stt = SarvamStreamingSTTProvider(
#         api_key=config.sarvam_api_key,
#         model=config.sarvam_stt_model
#     )

#     tts = SarvamStreamingTTSProvider(
#         api_key=config.sarvam_api_key,
#         model=config.sarvam_tts_model,
#         speaker=config.sarvam_speaker
#     )

#     voice_session = VoiceSession(agent=agent, tts=tts)

#     print("Pre-warming streaming providers...")
#     await asyncio.gather(
#         stt.connect(),
#         tts.connect()
#     )

#     # FIX: Create an isolated orchestration queue to separate speech processing from I/O
#     transcript_queue = asyncio.Queue()
#     active_response_task = None

#     try:
#         async def receive_audio():
#             """Inbound Client Microphone Stream Loop"""
#             try:
#                 while True:
#                     audio_chunk = await ws.receive_bytes()
#                     await stt.send_audio(audio_chunk)
#             except WebSocketDisconnect:
#                 raise
#             except Exception as e:
#                 logger.error(f"Error reading client audio bytes: {e}")
                
#         async def generate_and_send_response(transcript_text: str):
#             """Outbound Assistant Audio Generation Loop"""
#             try:
#                 async for audio_chunk in voice_session.process_transcript_to_audio(transcript_text):
#                     if not audio_chunk:
#                         continue

#                     if isinstance(audio_chunk, bytes):
#                         audio_payload = base64.b64encode(audio_chunk).decode("utf-8")
#                     else:
#                         audio_payload = audio_chunk

#                     if isinstance(audio_payload, str) and audio_payload.strip():
#                         await ws.send_json({
#                             "type": "audio",
#                             "audio": audio_payload
#                         })
#             except asyncio.CancelledError:
#                 logger.info("Response generation task explicitly cancelled.")
#             except Exception as e:
#                 if "1000" in str(e) or "408" in str(e):
#                     print("TTS connection dropped mid-stream. Resetting socket object reference.")
#                     tts.socket = None
#                 logger.error(f"Error in response generation execution: {e}")

#         async def process_transcripts():
#             """Inbound STT Stream Reader Loop"""
#             async for transcript_data in stt.stream_transcripts():
#                 transcript = transcript_data["text"].strip()
#                 if not transcript:
#                     continue

#                 print(f"USER: {transcript}")
#                 await ws.send_json({"type": "transcript", "text": transcript})
                
#                 # Push into our thread-safe queue instead of manually building overlapping tasks
#                 await transcript_queue.put(transcript)

#         async def dispatch_responses():
#             """Dedicated Queue Consumer Task to guarantee CPU execution attention"""
#             nonlocal active_response_task
#             while True:
#                 try:
#                     # Safely wait for the next text item to arrive in the background
#                     transcript = await transcript_queue.get()
                    
#                     if active_response_task and not active_response_task.done():
#                         print("Interrupting active speaking block for new phrase...")
#                         active_response_task.cancel()
#                         try:
#                             await active_response_task
#                         except asyncio.CancelledError:
#                             pass
#                         try:
#                             await tts.flush()
#                         except Exception:
#                             pass

#                     # Execute generation sequentially with complete scheduler priority
#                     active_response_task = asyncio.create_task(generate_and_send_response(transcript))
#                     await active_response_task
#                     transcript_queue.task_done()
                    
#                 except asyncio.CancelledError:
#                     break
#                 except Exception as e:
#                     logger.error(f"Queue Orchestration failure context: {e}")

#         # Initialize background streaming event loops
#         receive_task = asyncio.create_task(receive_audio())
#         transcript_task = asyncio.create_task(process_transcripts())
#         dispatch_task = asyncio.create_task(dispatch_responses())

#         # Keep everything executing alive until a critical network drop occurs
#         done, pending = await asyncio.wait(
#             [receive_task, transcript_task, dispatch_task],
#             return_when=asyncio.FIRST_COMPLETED
#         )

#         for task in done:
#             task.result()

#     except WebSocketDisconnect:
#         print("Client disconnected gracefully.")
#     except Exception as e:
#         logger.exception(e)
#         try:
#             await ws.send_json({"type": "error", "message": str(e)})
#         except Exception:
#             pass
#     finally:
#         # Rigid clean up teardown layout
#         if active_response_task and not active_response_task.done():
#             active_response_task.cancel()
        
#         for task_var in ['receive_task', 'transcript_task', 'dispatch_task']:
#             if task_var in locals():
#                 task_obj = locals()[task_var]
#                 if not task_obj.done():
#                     task_obj.cancel()

#         try:
#             await stt.close()
#             await tts.close()
#         except Exception:
#             pass
#         print("Voice Session Closed Safely")


