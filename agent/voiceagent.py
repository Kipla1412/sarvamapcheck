
from __future__ import annotations
import re
import base64
from agent.events import AgentEventType

class VoiceSession:

    def __init__(self, agent, tts):
        self.agent = agent
        self.tts = tts

    async def _drain_tts(self):
        """Drains audio chunks for the current segment until a final event frame is read."""
        while True:
            try:
                res = await self.tts.receive_audio()
                if res is None:
                    break

                # DEBUG: See exactly what Sarvam is returning
                print(f"Raw Frame Type: {type(res)} | Content: {res}")

                if hasattr(res, "type") and res.type == "event":
                    event_data = getattr(res, "data", None)
                    if event_data and getattr(event_data, "event_type", None) == "final":
                        print("Sarvam finished processing this sentence block.")
                        break

                if hasattr(res, "data") and res.data and hasattr(res.data, "audio"):
                    if res.data.audio:
                        yield base64.b64decode(res.data.audio)

            except Exception as e:
                print(f"TTS Drain Error: {e}")
                break

    async def process_transcript_to_audio(self, transcript: str):
        sentence_buffer = ""

        async for event in self.agent.run(transcript):
            if event.type == AgentEventType.TEXT_DELTA:
                token = event.data["content"]
                sentence_buffer += token

                # If we hit a natural sentence boundary, send it immediately to TTS
                if re.search(r"[.!?]\s*$", sentence_buffer):
                    if sentence_buffer.strip():
                        print(f"Sending to TTS: {sentence_buffer.strip()}")
                        await self.tts.send_text(sentence_buffer)
                        await self.tts.flush()
                        
                        async for audio_bytes in self._drain_tts():
                            yield audio_bytes
                        
                        sentence_buffer = ""

        # Flush any trailing tokens left in the buffer at completion
        if sentence_buffer.strip():
            print(f"Sending trailing buffer to TTS: {sentence_buffer.strip()}")
            await self.tts.send_text(sentence_buffer)
            await self.tts.flush()
            async for audio_bytes in self._drain_tts():
                yield audio_bytes

    async def text_to_audio(self, text: str):
        await self.tts.send_text(text)
        await self.tts.flush()

        async for audio_bytes in self._drain_tts():
            yield audio_bytes

            
# from __future__ import annotations
# import re
# import base64
# from agent.events import AgentEventType
# from websockets.exceptions import ConnectionClosedOK

# class VoiceSession:

#     def __init__(self, agent, tts):
#         self.agent = agent
#         self.tts = tts

#     async def _drain_tts(self):

#         while True:

#             try:

#                 res = await self.tts.receive_audio()
#                 print("TTS Response:", res)

#                 if res is None:
#                     print("TTS Response is None")
#                     break

#                 if (
#                     hasattr(res, "data")
#                     and res.data
#                     and hasattr(res.data, "audio")
#                 ):
#                     print("Audio chunk received")
#                     yield base64.b64decode(
#                         res.data.audio
#                     )

#             except ConnectionClosedOK:

#                 print(
#                     "TTS Stream Completed"
#                 )

#                 break

#             except Exception as e:

#                 print(
#                     f"TTS Error: {e}"
#                 )

#                 break

#     async def process_transcript_to_audio(self, transcript: str):
#         sentence_buffer = ""

#         async for event in self.agent.run(transcript):
#             if event.type == AgentEventType.TEXT_DELTA:
#                 token = event.data["content"]
#                 sentence_buffer += token

#                 # If we hit a natural sentence boundary, send it immediately to TTS
#                 if re.search(r"[.!?]\s*$", sentence_buffer):
#                     if sentence_buffer.strip():
#                         print("Transcript:", transcript)
#                         await self.tts.send_text(sentence_buffer)
#                         print(
#                             "Sending to TTS:",
#                             sentence_buffer
#                         )
#                         await self.tts.flush()
#                         print("TTS Flush Complete")
#                         sentence_buffer = ""

#                         # Drain all audio for this sentence
#                         async for audio_bytes in self._drain_tts():
#                             yield audio_bytes

#         # Flush any trailing tokens left in the buffer at completion
#         if sentence_buffer.strip():
#             await self.tts.send_text(sentence_buffer)
#             await self.tts.flush()
#             async for audio_bytes in self._drain_tts():
#                 yield audio_bytes
#     # async def process_transcript(self, transcript: str):

#     #     response_text = ""

#     #     async for event in self.agent.run(transcript):

#     #         if event.type == AgentEventType.TEXT_DELTA:
#     #             response_text += event.data["content"]

#     #     return response_text

#     async def text_to_audio(
#         self,
#         text: str
#     ):

#         await self.tts.send_text(text)

#         await self.tts.flush()

#         while True:

#             try:

#                 response = (
#                     await self.tts.receive_audio()
#                 )

#                 if (
#                     response
#                     and response.type == "audio"
#                 ):

#                     yield response.data.audio

#             except Exception:
#                 break