import asyncio
import numpy as np
import time

class SpeechToTextAgent:

    def __init__(self, engine):
        self.engine = engine

    async def run(self, audio_bytes: bytes, rate: int = 16000) -> str:
        """
        Input: raw PCM16 audio bytes
        Output: transcribed text
        """
        if not audio_bytes or len(audio_bytes) < 1000:
            return ""
            
        try:
            # Convert bytes → numpy
            audio = (
                np.frombuffer(audio_bytes, dtype=np.int16)
                .astype(np.float32) / 32768.0
            )

            # Normalize
            max_val = np.max(np.abs(audio))
            if max_val > 0:
                audio = audio / max_val
            # Process audio (mono + resample)
            processed = self.engine.processor.process(audio, rate)

            # Convert to WAV bytes
            wav_bytes = self.engine.processor.to_bytes(processed)

            # Transcribe
            start = time.time()

            text = await asyncio.to_thread(
                self.engine.provider.transcribe,
                wav_bytes
            )

            print(f"STT time: {time.time() - start:.2f}s")

            text = text.strip() if text else ""

            if len(text.split()) == 0:
                return ""

            return text
        except Exception as e:
            print("STT Error:", e)
            return ""