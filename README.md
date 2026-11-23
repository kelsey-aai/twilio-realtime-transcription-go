# Transcribe Twilio phone calls in real time with Go

Companion code for the AssemblyAI tutorial [Transcribe phone calls in real time in Go with Twilio and AssemblyAI](https://www.assemblyai.com/blog/transcribe-phone-calls-in-realtime-in-go-with-twilio-and-assemblyai).

A Go server answers your Twilio number with `<Connect><Stream>`, receives the call audio over a WebSocket, and streams it to AssemblyAI-STT-3.7-Realtime over a raw WebSocket (`wss://streaming.assemblyai.com/v3/ws?speech_model=assemblyai-stt-3-7-realtime`). AssemblyAI has no Go SDK, so this calls the streaming API directly. Partial transcripts update one console line; each final transcript is printed on its own line.

Twilio Media Streams audio is 8 kHz mono mu-law. The server sends it as-is (`encoding=pcm_mulaw`, `sample_rate=8000`) with no resampling.

## Quick start

```bash
git clone https://github.com/kelsey-aai/twilio-realtime-transcription-go.git
cd twilio-realtime-transcription-go
cp .env.example .env   # add your ASSEMBLYAI_API_KEY
go run .
```

In a second terminal, run `ngrok http 8080` and set your Twilio number's "A call comes in" webhook to the HTTPS forwarding URL (HTTP POST). Then call the number and speak.

## Configuration

| Variable | Required | What it does |
|---|---|---|
| `ASSEMBLYAI_API_KEY` | Yes | Your AssemblyAI API key, sent in the `Authorization` header |
| `KEYTERMS` | No | Comma-separated terms to boost, sent as one JSON-encoded `keyterms_prompt` query parameter (up to 100 terms) |
| `PORT` | No | Local port, default `8080` |

Requires Go 1.21 or later.

## Docs

- [AssemblyAI streaming model selection](https://www.assemblyai.com/docs/streaming/select-the-speech-model)
- [Streaming message sequence](https://www.assemblyai.com/docs/streaming/message-sequence)
- [Twilio Media Streams WebSocket messages](https://www.twilio.com/docs/voice/media-streams/websocket-messages)
