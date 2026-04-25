# import asyncio
# import sounddevice as sd
# import numpy as np

# from config.config import Config
# from src.speechtospeech.speechtotext.streamtranscriber import StreamTranscriber


# RATE = 16000
# CHUNK_DURATION = 1  # seconds


# async def main():

#     config = Config()
#     engine = config.stt_engine

#     streamer = StreamTranscriber(engine)

#     print("🎤 Live streaming started...")

#     while True:

#         # 🎙️ record chunk
#         audio = sd.rec(
#             int(CHUNK_DURATION * RATE),
#             samplerate=RATE,
#             channels=1
#         )
#         sd.wait()

#         audio = np.squeeze(audio)

#         result = await streamer.process_chunk(audio, RATE)

#         if result:

#             if "partial" in result:
#                 print("Partial:", result["partial"])

#             if "final" in result:
#                 print("Final:", result["final"])


# asyncio.run(main())

# import asyncio
# from agent.stt import SpeechToTextAgent
# from config.config import Config

# # Load config (make sure your stt_engine is configured)
# config = Config()

# # Create STT agent
# stt = SpeechToTextAgent(config.stt_engine)


# async def main():
#     # Read audio file (WAV - PCM16, 16kHz recommended)
#     with open("test/output.wav", "rb") as f:
#         audio_bytes = f.read()

#     print("Testing STT...")

#     text = await stt.run(audio_bytes)

#     print("Transcription Result:")
#     print(text)


# if __name__ == "__main__":
#     asyncio.run(main())

import asyncio
import sounddevice as sd
import numpy as np
import time

from config.config import Config
from agent.stt import SpeechToTextAgent


class MicStreamer:

    def __init__(self, stt_agent):
        self.stt_agent = stt_agent
        self.buffer = []
        self.last_speech_time = time.time()

    def is_speech(self, audio_chunk):
        audio = audio_chunk.astype(np.float32) / 32768.0
        energy = np.mean(np.abs(audio))
        return energy > 0.01

    async def process(self, chunk_bytes):
        self.buffer.append(chunk_bytes)

        if self.is_speech(np.frombuffer(chunk_bytes, dtype=np.int16)):
            self.last_speech_time = time.time()

        # silence detected
        if time.time() - self.last_speech_time > 0.8:
            if not self.buffer:
                return None

            full_audio = b"".join(self.buffer)
            self.buffer = []

            text = await self.stt_agent.run(full_audio)
            return text

        return None


async def main():
    config = Config()
    stt = SpeechToTextAgent(config.stt_engine)
    
    # Check if audio devices are available
    try:
        devices = sd.query_devices()
        if len(devices) == 0:
            print("❌ No audio devices found. Running in test mode with simulated audio...")
            await test_with_simulated_audio(stt)
            return
    except Exception as e:
        print(f"❌ Error checking audio devices: {e}")
        print("Running in test mode with simulated audio...")
        await test_with_simulated_audio(stt)
        return
    
    streamer = MicStreamer(stt)

    samplerate = 16000
    blocksize = 1600  # ~100ms

    print("🎤 Speak now...")

    loop = asyncio.get_running_loop()

    def callback(indata, frames, time_info, status):
        audio_bytes = indata.tobytes()

        asyncio.run_coroutine_threadsafe(
            handle_chunk(audio_bytes), loop
        )

    async def handle_chunk(chunk):
        text = await streamer.process(chunk)
        if text:
            print("🧾:", text)
    
    device = None

    devices = sd.query_devices()
    for i, d in enumerate(devices):
        if d['max_input_channels'] > 0:
            device = i
            print("Using input device:", d['name'])
            break
    try:
        with sd.InputStream(
            samplerate=samplerate,
            channels=1,
            dtype="int16",
            blocksize=blocksize,
            device=device,
            callback=callback,
        ):
            await asyncio.Event().wait()
    except sd.PortAudioError as e:
        print(f"❌ Audio stream error: {e}")
        print("Falling back to test mode with simulated audio...")
        await test_with_simulated_audio(stt)


async def test_with_simulated_audio(stt_agent):
    """Test STT with simulated audio data when no microphone is available"""
    print("🧪 Testing STT engine with simulated audio...")
    
    # Generate some silent audio data (16-bit PCM, 16kHz, 1 second)
    duration = 1.0  # seconds
    samplerate = 16000
    samples = int(duration * samplerate)
    
    # Create silent audio (all zeros)
    silent_audio = np.zeros(samples, dtype=np.int16)
    audio_bytes = silent_audio.tobytes()
    
    try:
        print("Processing simulated audio...")
        text = await stt_agent.run(audio_bytes)
        print("🧾 Transcription result:", text or "(no speech detected)")
        print("✅ STT engine test completed successfully")
    except Exception as e:
        print(f"❌ Error during STT processing: {e}")
        print("Please check your STT configuration")


if __name__ == "__main__":
    asyncio.run(main())