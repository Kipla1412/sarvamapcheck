# Taste (Continuously Learned by [CommandCode][cmd])

[cmd]: https://commandcode.ai/

# workflow
- Before making any code change, get explicit line-by-line permission from the user first. Confidence: 0.85

# voice-assistant
- Voice speaking output should match the patient's language, but assistant text response/logs should always be in English. Confidence: 0.90
- Stream text tokens to UI immediately for low latency, but batch TTS audio (multiple sentences ≥ 60 chars) to avoid unnatural pauses at every sentence boundary. Confidence: 0.75

# documentation
- For team/colleague reference documentation, prefer adding inline print/log statements in the actual code showing protocol messages as they flow, rather than creating separate docs files. Confidence: 0.70
- When printing streamed text tokens for debugging/demo, condense them (show the final concatenated message) instead of printing every individual token one by one. Confidence: 0.65
- When printing streamed audio chunks for debugging/demo, condense them into a single print per response but still show the actual message format/structure (e.g., the type and audio keys) rather than just a bytes/chunks-only summary. Confidence: 0.70

