import numpy as np
import webrtcvad

# class VoiceActivityDetector:
#     """
#     Simple WebRTC Voice Activity Detector.
#     """

#     def __init__(self, aggressiveness: int = 2, sample_rate: int = 16000):
#         """
#         aggressiveness: 0-3 (higher = more aggressive filtering)
#         sample_rate: audio sample rate
#         """
#         self.sample_rate = sample_rate
#         # Energy threshold based on aggressiveness
#         self.energy_threshold = 0.001 * (4 - aggressiveness)  # Higher aggressiveness = lower threshold
#         self.min_duration = 0.01  # Minimum 10ms of speech

#     def is_speech(self, audio_chunk: bytes) -> bool:
#         """
#         Detect if chunk contains speech using energy.

#         Args:
#             audio_chunk: raw PCM int16 bytes

#         Returns:
#             True if speech detected
#         """
#         try:
#             # Convert bytes to numpy array
#             audio = np.frombuffer(audio_chunk, dtype=np.int16).astype(np.float32) / 32768.0
            
#             # Calculate RMS energy
#             energy = np.sqrt(np.mean(audio ** 2))
            
#             # Use a more reasonable threshold
#             # Normal speech has energy around 0.01-0.1, silence is < 0.001
#             return energy > self.energy_threshold
            
#         except Exception:
#             return False

import webrtcvad


class VoiceActivityDetector:
    """
    WebRTC Voice Activity Detector
    """

    def __init__(self, aggressiveness: int = 0, sample_rate: int = 16000):
        self.sample_rate = sample_rate
        self.vad = webrtcvad.Vad(aggressiveness)

    def is_speech(self, audio_chunk: bytes) -> bool:
        try:
            # WebRTC VAD requires specific frame sizes (20ms = 640 bytes at 16kHz)
            frame_duration_ms = 20
            frame_size = int(self.sample_rate * frame_duration_ms / 1000) * 2  # 640 bytes

            if len(audio_chunk) < frame_size:
                return False

            # Process all complete frames in the chunk
            speech_frames = 0
            total_frames = 0
            
            for i in range(0, len(audio_chunk) - frame_size + 1, frame_size):
                frame = audio_chunk[i:i + frame_size]
                total_frames += 1
                if self.vad.is_speech(frame, self.sample_rate):
                    speech_frames += 1
            
            # Consider it speech if at least 30% of frames contain speech
            return total_frames > 0 and (speech_frames / total_frames) >= 0.3

        except Exception:
            return False