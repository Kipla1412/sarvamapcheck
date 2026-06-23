from __future__ import annotations
import base64
from typing import AsyncGenerator
from sarvamai import AsyncSarvamAI

class SarvamStreamingSTTProvider:

    def __init__(
        self,
        api_key: str,
        language_code: str = "en-IN",
        model: str = "saaras:v3",
        sample_rate: int = 16000,
    ):
        self.client = AsyncSarvamAI(api_subscription_key=api_key)
        self.language_code = language_code
        self.model = model
        self.sample_rate = sample_rate

        self.socket = None
        self.ctx = None

    async def connect(self):
        self.ctx = self.client.speech_to_text_streaming.connect(
            model=self.model,
            mode="transcribe",
            language_code=self.language_code,
            sample_rate=self.sample_rate,
            input_audio_codec="pcm",
            high_vad_sensitivity=True
        )
        self.socket = await self.ctx.__aenter__()
        print("Sarvam STT Connected")

    async def send_audio(self, audio_bytes: bytes):
        if not audio_bytes or self.socket is None:
            return

        try:
            audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
            await self.socket.transcribe(audio=audio_b64)
        except Exception as e:
            print(f"STT Write Error: {e}")
            self.socket = None

    async def stream_transcripts(self) -> AsyncGenerator[dict, None]:
        while True:
            if self.socket is None:
                break
            try:
                response = await self.socket.recv()
                if response and hasattr(response, "data") and response.data:
                    yield {
                        "text": getattr(response.data, "transcript", "").strip(),
                        "is_final": getattr(response.data, "is_final", True),
                        "request_id": getattr(response.data, "request_id", None),
                        "language": getattr(response.data, "language_code", self.language_code),
                    }
            except Exception as e:
                print(f"STT Stream Read Error: {e}")
                break

    async def flush(self):
        if self.socket:
            try:
                await self.socket.flush()
            except Exception:
                pass

    async def close(self):
        if self.ctx:
            try:
                await self.ctx.__aexit__(None, None, None)
            except Exception:
                pass
        self.ctx = None
        self.socket = None
        print("Sarvam STT Closed")

