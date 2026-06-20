import asyncio
import os
import base64

from dotenv import load_dotenv
from speechtospeech.providers.tts.streamsarvam import SarvamStreamingTTSProvider

load_dotenv()


async def main():

    tts = SarvamStreamingTTSProvider(
        api_key=os.getenv("SARVAM_API_KEY")
    )

    await tts.connect()

    await tts.send_text(
        "Hello Umar. How are you today?"
    )

    await tts.flush()

    audio_bytes = b""

    while True:

        response = await tts.receive_audio()

        print(type(response))
        print(response.type)

        if response.type == "audio":

            audio_bytes += base64.b64decode(
                response.data.audio
            )

        elif response.type == "error":

            print("TTS Completed")
            break

    with open("output.mp3", "wb") as f:
        f.write(audio_bytes)

    print("Saved output.mp3")
    print(
    f"Saved {len(audio_bytes)} bytes"
)

    await tts.close()


asyncio.run(main())