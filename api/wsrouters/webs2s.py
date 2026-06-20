from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import asyncio
import logging
import base64

from agent.agent import Agent
from agent.voiceagent import VoiceSession
from speechtospeech.providers.stt.streamsarvam import SarvamStreamingSTTProvider
from speechtospeech.providers.tts.streamsarvam import SarvamStreamingTTSProvider

logger = logging.getLogger(__name__)
router = APIRouter()

@router.websocket("/audio")
async def audio_stream(ws: WebSocket):

    await ws.accept()
    print("🎙️ Voice Session Started")

    agent: Agent = ws.app.state.agent
    config = ws.app.state.config

    stt = SarvamStreamingSTTProvider(
        api_key=config.sarvam_api_key,
        model=config.sarvam_stt_model
    )

    tts = SarvamStreamingTTSProvider(
        api_key=config.sarvam_api_key,
        model=config.sarvam_tts_model,
        speaker=config.sarvam_speaker
    )

    voice_session = VoiceSession(
        agent=agent,
        tts=tts
    )

    await stt.connect()
    await tts.connect()

    active_response_task = None

    try:
        async def receive_audio():
            try:
                while True:
                    audio_chunk = await ws.receive_bytes()
                    await stt.send_audio(audio_chunk)
            except WebSocketDisconnect:
                raise
            except Exception as e:
                logger.error(f"Error reading client audio bytes: {e}")
                
        async def generate_and_send_response(transcript_text: str):
            try:
                async for audio_chunk in voice_session.process_transcript_to_audio(transcript_text):
                    if isinstance(audio_chunk, bytes):
                        audio_payload = base64.b64encode(audio_chunk).decode("utf-8")
                    else:
                        audio_payload = audio_chunk

                    await ws.send_json({
                        "type": "audio",
                        "audio": audio_payload
                    })
            except asyncio.CancelledError:
                logger.info("Response generation task explicitly cancelled.")
            except Exception as e:
                logger.error(f"Error in response generation: {e}")

        async def process_transcripts():
            nonlocal active_response_task
            async for transcript_data in stt.stream_transcripts():
                transcript = transcript_data["text"].strip()

                if not transcript:
                    continue

                print(f"USER: {transcript}")

                await ws.send_json({
                    "type": "transcript",
                    "text": transcript
                })

                if active_response_task and not active_response_task.done():
                    print("AI still speaking...")
                    continue
                # if active_response_task and not active_response_task.done():
                #     active_response_task.cancel()
                #     try:
                #         await asyncio.wait_for(tts.flush(), timeout=0.5) 
                #     except Exception:
                #         pass
                        
                #     print("🛑 AI Interrupted by User")

                active_response_task = asyncio.create_task(
                    generate_and_send_response(transcript)
                )

        # Create explicit background tasks
        receive_task = asyncio.create_task(receive_audio())
        transcript_task = asyncio.create_task(process_transcripts())

        # Wait for either task to finish/fail
        done, pending = await asyncio.wait(
            [receive_task, transcript_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        # If the browser disconnected, clean up immediately
        if receive_task in done:
            for task in done:
                task.result()
        else:
            # STT disconnected but browser is still connected.
            # Let the active response finish before closing.
            if active_response_task and not active_response_task.done():
                logger.info("STT disconnected, waiting for active response to finish...")
                try:
                    await asyncio.wait_for(active_response_task, timeout=30.0)
                except asyncio.TimeoutError:
                    logger.warning("Response generation timed out after STT disconnect")
                    active_response_task.cancel()
                except Exception as e:
                    logger.error(f"Response generation error after STT disconnect: {e}")

    except WebSocketDisconnect:
        print("Client disconnected")

    except Exception as e:
        logger.exception(e)
        try:
            await ws.send_json({
                "type": "error",
                "message": str(e)
            })
        except Exception:
            pass

    finally:
        # Let the active response finish if it's still going
        if active_response_task and not active_response_task.done():
            try:
                await asyncio.wait_for(active_response_task, timeout=5.0)
            except Exception:
                active_response_task.cancel()
            
        if 'receive_task' in locals() and not receive_task.done():
            receive_task.cancel()
            
        if 'transcript_task' in locals() and not transcript_task.done():
            transcript_task.cancel()

        await stt.close()
        await tts.close()
        print("🔌 Voice Session Closed")

# from fastapi import APIRouter, WebSocket, WebSocketDisconnect
# import asyncio
# import logging

# from agent.agent import Agent
# from agent.events import AgentEventType
# from agent.voiceagent import VoiceSession
# from speechtospeech.providers.stt.streamsarvam import (
#     SarvamStreamingSTTProvider
# )

# from speechtospeech.providers.tts.streamsarvam import (
#     SarvamStreamingTTSProvider
# )

# logger = logging.getLogger(__name__)
# router = APIRouter()

# @router.websocket("/audio")
# async def audio_stream(ws: WebSocket):

#     await ws.accept()
#     print("🎙️ Voice Session Started")

#     agent: Agent = ws.app.state.agent
#     config = ws.app.state.config

#     stt = SarvamStreamingSTTProvider(
#         api_key=config.sarvam_api_key,
#         model=config.sarvam_stt_model
#     )

#     tts = SarvamStreamingTTSProvider(
#         api_key=config.sarvam_api_key,
#         model=config.sarvam_tts_model,
#         speaker=config.sarvam_speaker
#     )

#     voice_session = VoiceSession(
#         agent=agent,
#         tts=tts
#     )

#     await stt.connect()
#     await tts.connect()

#     # Track active agent response tasks so we can manage them
#     active_response_task = None

#     try:
#         async def receive_audio():
#             while True:
#                 audio_chunk = await ws.receive_bytes()
#                 await stt.send_audio(audio_chunk)
                
#         async def generate_and_send_response(transcript_text: str):
#             """Helper function to stream audio without blocking the STT loop"""
#             try:
#                 async for audio_chunk in voice_session.process_transcript_to_audio(transcript_text):
#                     await ws.send_json({
#                         "type": "audio",
#                         "audio": audio_chunk
#                     })
#             except Exception as e:
#                 logger.error(f"Error in response generation: {e}")

#         async def process_transcripts():
#             nonlocal active_response_task
#             async for transcript_data in stt.stream_transcripts():
#                 transcript = transcript_data["text"].strip()

#                 if not transcript:
#                     continue

#                 print(f"USER: {transcript}")

#                 await ws.send_json({
#                     "type": "transcript",
#                     "text": transcript
#                 })

#                 # 🌟 INTERRUPTION HANDLING (Optional but highly recommended)
#                 # If the user speaks while the AI is already processing a previous thought,
#                 # cancel the old response task instantly!
#                 if active_response_task and not active_response_task.done():
#                     active_response_task.cancel()
#                     await tts.flush() # Clear out pending audio buffers
#                     print("🛑 AI Interrupted by User")

#                 # Run the response generator as a background task
#                 active_response_task = asyncio.create_task(
#                     generate_and_send_response(transcript)
#                 )

#         await asyncio.gather(
#             receive_audio(),
#             process_transcripts(),
#         )

#     except WebSocketDisconnect:
#         print("Client disconnected")

#     except Exception as e:
#         logger.exception(e)
#         await ws.send_json({
#             "type": "error",
#             "message": str(e)
#         })

#     finally:
#         # Clean up any remaining background jobs
#         if active_response_task and not active_response_task.done():
#             active_response_task.cancel()

#         await stt.close()
#         await tts.close()
#         print("🔌 Voice Session Closed")

# @router.websocket("/audio")
# async def audio_stream(ws: WebSocket):

#     await ws.accept()

#     print(" Voice Session Started")

#     agent: Agent = ws.app.state.agent
#     config = ws.app.state.config

#     stt = SarvamStreamingSTTProvider(
#         api_key=config.sarvam_api_key,
#         model=config.sarvam_stt_model
#     )

#     tts = SarvamStreamingTTSProvider(
#         api_key=config.sarvam_api_key,
#         model=config.sarvam_tts_model,
#         speaker=config.sarvam_speaker
#     )

#     voice_session = VoiceSession(
#         agent=agent,
#         tts=tts
#     )

#     await stt.connect()
#     await tts.connect()

#     try:

#         async def receive_audio():

#             while True:

#                 audio_chunk = await ws.receive_bytes()

#                 await stt.send_audio(
#                     audio_chunk
#                 )

#         async def process_transcripts():
#             async for transcript_data in stt.stream_transcripts():
#                 transcript = transcript_data["text"].strip()

#                 if not transcript:
#                     continue

#                 print(f"USER: {transcript}")

#                 await ws.send_json({
#                     "type": "transcript",
#                     "text": transcript
#                 })

#                 # Stream audio blocks back to the client as they are generated
#                 async for audio_chunk in voice_session.process_transcript_to_audio(transcript):
#                     await ws.send_json({
#                         "type": "audio",
#                         "audio": audio_chunk
#                     })

#         # async def process_transcripts():

#         #     async for transcript_data in (
#         #         stt.stream_transcripts()
#         #     ):
                


#         #         transcript = (
#         #             transcript_data["text"]
#         #             .strip()
#         #         )

#         #         if not transcript:
#         #             continue

#         #         print(
#         #             f"USER: {transcript}"
#         #         )

#         #         await ws.send_json({
#         #             "type": "transcript",
#         #             "text": transcript
#         #         })

#         #         response_text = await voice_session.process_transcript(transcript)

#         #         async for audio in (
#         #             voice_session.text_to_audio(
#         #                 response_text
#         #             )
#         #         ):

#         #             await ws.send_json({
#         #                 "type": "audio",
#         #                 "audio": audio
#         #             })
#                 # response_text = ""

#                 # async for event in agent.run(
#                 #     transcript
#                 # ):

#                 #     if (
#                 #         event.type
#                 #         == AgentEventType.TEXT_DELTA
#                 #     ):

#                 #         token = (
#                 #             event.data["content"]
#                 #         )

#                 #         response_text += token

#                 #         await ws.send_json({
#                 #             "type": "text_delta",
#                 #             "text": token
#                 #         })

#                 # print(
#                 #     f"ASSISTANT: {response_text}"
#                 # )

#                 # await tts.send_text(
#                 #     response_text
#                 # )

#                 # await tts.flush()

#                 # while True:

#                 #     try:

#                 #         response = (
#                 #             await tts.receive_audio()
#                 #         )

#                 #         if (
#                 #             response.type
#                 #             == "audio"
#                 #         ):

#                 #             await ws.send_json({
#                 #                 "type": "audio",
#                 #                 "audio":
#                 #                 response.data.audio
#                 #             })

#                 #     except Exception:
#                 #         break

#         await asyncio.gather(
#             receive_audio(),
#             process_transcripts(),
#         )

#     except WebSocketDisconnect:

#         print(
#             "Client disconnected"
#         )

#     except Exception as e:

#         logger.exception(e)

#         await ws.send_json({
#             "type": "error",
#             "message": str(e)
#         })

#     finally:

#         await stt.close()
#         await tts.close()

#         print(
#             "🔌 Voice Session Closed"
#         )

# from fastapi import APIRouter, WebSocket, WebSocketDisconnect
# import base64
# import json
# import logging
# from agent.events import AgentEventType
# from agent.session import Session
# from agent.agent import Agent
# from api.auth import decode_token
# import time


# logger = logging.getLogger(__name__)
# router = APIRouter()


# @router.websocket("/audio")
# async def audio_stream(ws: WebSocket):

#     # AUTH (your existing logic)
#     token = None

#     auth_header = ws.headers.get("Authorization")
#     if auth_header and auth_header.startswith("Bearer "):
#         token = auth_header.split(" ")[1]

#     if not token:
#         token = ws.query_params.get("token")

#     if not token:
#         await ws.close(code=1008)
#         return

#     try:
#         user = decode_token(token)
#         ws.state.user = user
#     except Exception:
#         await ws.close(code=1008)
#         return

#     await ws.accept()

#     user_id = str(user.get("id"))

#     print(f"🎤 Connected user: {user_id}")

#     agent: Agent = ws.app.state.agent
#     sessions = ws.app.state.sessions

#     # CREATE / GET USER SESSION
#     if user_id not in sessions:
#         sessions[user_id] = Session(ws.app.state.config)
#         await sessions[user_id].initialize()

#     session = sessions[user_id]

#     try:
#         while True:
#             audio_chunk = await ws.receive_bytes()
#             session.last_activity = time.time()  # ADD THIS

#             # IMPORTANT: pass session
#             result = await agent.run_audio(
#                 audio_chunk,
#                 rate=16000,
#                 session=session
#             )

#             if result:

#                 print(f"USER: {result}")

#                 response_buffer = ""

#                 async for event in agent.run(result):

#                     if event.type == AgentEventType.TEXT_DELTA:

#                         token = event.data["content"]

#                         response_buffer += token

#                         await ws.send_json({
#                             "type": "text_delta",
#                             "text": token
#                         })

#                     elif event.type == AgentEventType.TEXT_COMPLETE:

#                         print(
#                             f"ASSISTANT: {response_buffer}"
#                         )

#     except WebSocketDisconnect:
#         print(f"{user_id} disconnected")

#         # CLEANUP (important)
#         if user_id in sessions:
#             if sessions[user_id].client:
#                 await sessions[user_id].client.close()
#             del sessions[user_id]

#     except Exception as e:
#         print("Runtime error:", e)
#         await ws.send_json({
#             "type": "error",
#             "message": str(e)
#         })

