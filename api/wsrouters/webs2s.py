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
    print("Voice Session Started")

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

    voice_session = VoiceSession(agent=agent, tts=tts)

    print("Pre-warming streaming providers...")
    await asyncio.gather(
        stt.connect(),
        tts.connect()
    )

    # FIX: Create an isolated orchestration queue to separate speech processing from I/O
    transcript_queue = asyncio.Queue()
    active_response_task = None

    try:
        async def receive_audio():
            """Inbound Client Microphone Stream Loop"""
            try:
                while True:
                    audio_chunk = await ws.receive_bytes()
                    await stt.send_audio(audio_chunk)
            except WebSocketDisconnect:
                raise
            except Exception as e:
                logger.error(f"Error reading client audio bytes: {e}")
                
        async def generate_and_send_response(transcript_text: str):
            """Outbound Assistant Audio Generation Loop"""
            try:
                async for audio_chunk in voice_session.process_transcript_to_audio(transcript_text):
                    if not audio_chunk:
                        continue

                    if isinstance(audio_chunk, bytes):
                        audio_payload = base64.b64encode(audio_chunk).decode("utf-8")
                    else:
                        audio_payload = audio_chunk

                    if isinstance(audio_payload, str) and audio_payload.strip():
                        await ws.send_json({
                            "type": "audio",
                            "audio": audio_payload
                        })
            except asyncio.CancelledError:
                logger.info("Response generation task explicitly cancelled.")
            except Exception as e:
                if "1000" in str(e) or "408" in str(e):
                    print("TTS connection dropped mid-stream. Resetting socket object reference.")
                    tts.socket = None
                logger.error(f"Error in response generation execution: {e}")

        async def process_transcripts():
            """Inbound STT Stream Reader Loop"""
            async for transcript_data in stt.stream_transcripts():
                transcript = transcript_data["text"].strip()
                if not transcript:
                    continue

                print(f"USER: {transcript}")
                await ws.send_json({"type": "transcript", "text": transcript})
                
                # Push into our thread-safe queue instead of manually building overlapping tasks
                await transcript_queue.put(transcript)

        async def dispatch_responses():
            """Dedicated Queue Consumer Task to guarantee CPU execution attention"""
            nonlocal active_response_task
            while True:
                try:
                    # Safely wait for the next text item to arrive in the background
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

                    # Execute generation sequentially with complete scheduler priority
                    active_response_task = asyncio.create_task(generate_and_send_response(transcript))
                    await active_response_task
                    transcript_queue.task_done()
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Queue Orchestration failure context: {e}")

        # Initialize background streaming event loops
        receive_task = asyncio.create_task(receive_audio())
        transcript_task = asyncio.create_task(process_transcripts())
        dispatch_task = asyncio.create_task(dispatch_responses())

        # Keep everything executing alive until a critical network drop occurs
        done, pending = await asyncio.wait(
            [receive_task, transcript_task, dispatch_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            task.result()

    except WebSocketDisconnect:
        print("Client disconnected gracefully.")
    except Exception as e:
        logger.exception(e)
        try:
            await ws.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        # Rigid clean up teardown layout
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
        print("Voice Session Closed Safely")


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
#     print("Voice Session Started")

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

#                 # INTERRUPTION HANDLING (Optional but highly recommended)
#                 # If the user speaks while the AI is already processing a previous thought,
#                 # cancel the old response task instantly!
#                 if active_response_task and not active_response_task.done():
#                     active_response_task.cancel()
#                     await tts.flush() # Clear out pending audio buffers
#                     print("AI Interrupted by User")

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
#         print("Voice Session Closed")
