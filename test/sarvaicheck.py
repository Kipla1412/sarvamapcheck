# import asyncio
# from pathlib import Path

# from speechtospeech.providers.stt.sarvam import SarvamSTTProvider
# import os
from dotenv import load_dotenv
load_dotenv()

# async def main():
#     audio_path = Path(__file__).resolve().parent / "output.wav"

#     stt = SarvamSTTProvider(
#         api_key=os.getenv("SARVAM_API_KEY")
#     )

#     text = await stt.transcribe(
#         str(audio_path)
#     )

#     print(text)


# if __name__ == "__main__":
#     asyncio.run(main())

import asyncio
import os
from speechtospeech.providers.tts.sarvam import SarvamTTSProvider


# async def main():

#     tts = SarvamTTSProvider(
#         api_key=os.getenv("SARVAM_API_KEY")
#     )

#     audio_path = await tts.synthesize(
#         text="Hello Umar, how are you today?",
#         output_file="output/response.wav"
#     )

#     print(audio_path)


# if __name__ == "__main__":
#     asyncio.run(main())

from sarvamai import AsyncSarvamAI
import inspect
import asyncio
import base64
import sounddevice as sd
import numpy as np

from sarvamai import AsyncSarvamAI


async def main():

    client = AsyncSarvamAI(
        api_subscription_key=os.getenv("SARVAM_API_KEY")
    )

    async with client.text_to_speech_streaming.connect(
        model="bulbul:v3"
    ) as socket:

        print(
            inspect.signature(
                socket.configure
            )
        )

        print(
            inspect.signature(
                socket.convert
            )
        )

        print(
            inspect.signature(
                socket.recv
            )
        )

# asyncio.run(main())
    # async with client.speech_to_text_streaming.connect(
    #     language_code="en-IN",
    #     model="saaras:v3",
    #     sample_rate="16000",
    #     input_audio_codec="pcm_raw"
    # ) as socket:

    #     print("🎤 Connected to Sarvam STT")
    #     print("🎤 Start speaking...")

    #     async def send_audio():

    #         samplerate = 16000
    #         chunk_duration = 0.1  # 100ms

    #         stream = sd.InputStream(
    #             samplerate=samplerate,
    #             channels=1,
    #             dtype="int16"
    #         )

    #         stream.start()

    #         try:

    #             while True:

    #                 audio_chunk, overflowed = stream.read(
    #                     int(samplerate * chunk_duration)
    #                 )

    #                 audio_bytes = audio_chunk.tobytes()

    #                 audio_b64 = base64.b64encode(
    #                     audio_bytes
    #                 ).decode()

    #                 await socket.transcribe(
    #                     audio=audio_b64,
    #                     encoding="audio/wav",
    #                     sample_rate=16000
    #                 )

    #                 await asyncio.sleep(0.01)

    #         finally:
    #             stream.stop()
    #             stream.close()

    #     async def receive_transcripts():

    #         while True:

    #             try:

    #                 response = await socket.recv()

    #                 print("\nTRANSCRIPT:")
    #                 print(response)

    #             except Exception as e:
    #                 print("Receive Error:", e)
    #                 break

    #     await asyncio.gather(
    #         send_audio(),
    #         receive_transcripts()
    #     )


if __name__ == "__main__":
    asyncio.run(main())