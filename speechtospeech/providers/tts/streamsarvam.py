from __future__ import annotations
from sarvamai import AsyncSarvamAI


class SarvamStreamingTTSProvider:

    def __init__(
        self,
        api_key: str,
        language_code: str = "en-IN",
        speaker: str = "neha",
        model: str = "bulbul:v3",
    ):
        self.client = AsyncSarvamAI(
            api_subscription_key=api_key
        )

        self.language_code = language_code
        self.speaker = speaker
        self.model = model

        self.ctx = None
        self.socket = None

    async def connect(self):

        self.ctx = (
            self.client.text_to_speech_streaming.connect(
                model=self.model,
                send_completion_event=True
            )
        )

        self.socket = await self.ctx.__aenter__()

        await self.socket.configure(
            target_language_code=self.language_code,
            speaker=self.speaker,
            speech_sample_rate=24000,
            output_audio_codec="wav",
        )

        # await self.socket.start_listening()

        print("TTS Connected")

    async def send_text(
        self,
        text: str
    ):

        if not text.strip():
            return

        await self.socket.convert(text)

    async def receive_audio(self):
        """Returns the raw response from the Sarvam SDK, or None on error."""
        try:
            return await self.socket.recv()
        except Exception as e:
            print(f"TTS Receive Error: {e}")
            return None

    async def flush(self):
        await self.socket.flush()

    async def close(self):
        if self.ctx:
            await self.ctx.__aexit__(None, None, None)
            print("TTS Closed")