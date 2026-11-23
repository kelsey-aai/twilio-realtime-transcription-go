// Transcribe Twilio phone calls in real time with AssemblyAI-STT-3.7-Realtime.
// Raw WebSocket calls against wss://streaming.assemblyai.com/v3/ws (there is no Go SDK).
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

const (
	assemblyAIURL = "wss://streaming.assemblyai.com/v3/ws"
	speechModel   = "assemblyai-stt-3-7-realtime"
	sampleRate    = 8000 // Twilio Media Streams: 8 kHz mono mu-law
	bufferBytes   = 800  // 100 ms of mu-law audio (one byte per sample)
)

// TwilioMessage covers the Media Streams events this server reads.
type TwilioMessage struct {
	Event string `json:"event"`
	Start *struct {
		CallSid     string `json:"callSid"`
		MediaFormat struct {
			Encoding   string `json:"encoding"`
			SampleRate int    `json:"sampleRate"`
			Channels   int    `json:"channels"`
		} `json:"mediaFormat"`
	} `json:"start,omitempty"`
	Media *struct {
		Payload string `json:"payload"`
	} `json:"media,omitempty"`
}

// AssemblyAIMessage covers Begin, Turn, Termination and Error.
type AssemblyAIMessage struct {
	Type                 string  `json:"type"`
	ID                   string  `json:"id,omitempty"`
	Transcript           string  `json:"transcript,omitempty"`
	EndOfTurn            bool    `json:"end_of_turn,omitempty"`
	TurnOrder            int     `json:"turn_order,omitempty"`
	AudioDurationSeconds float64 `json:"audio_duration_seconds,omitempty"`
	ErrorCode            int     `json:"error_code,omitempty"`
	Error                string  `json:"error,omitempty"`
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

// twimlHandler answers Twilio's voice webhook with <Connect><Stream>.
func twimlHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say>Speak to see your words transcribed in the console.</Say>
  <Connect>
    <Stream url="wss://%s/media" />
  </Connect>
</Response>`, r.Host)
}

// connectAssemblyAI opens a streaming session configured for Twilio audio.
func connectAssemblyAI() (*websocket.Conn, error) {
	params := url.Values{}
	params.Set("speech_model", speechModel)
	params.Set("encoding", "pcm_mulaw")
	params.Set("sample_rate", fmt.Sprint(sampleRate))

	// Optional keyterms: ONE keyterms_prompt parameter holding a JSON array.
	if raw := os.Getenv("KEYTERMS"); raw != "" {
		var terms []string
		for _, t := range strings.Split(raw, ",") {
			if t = strings.TrimSpace(t); t != "" {
				terms = append(terms, t)
			}
		}
		encoded, err := json.Marshal(terms)
		if err != nil {
			return nil, err
		}
		params.Set("keyterms_prompt", string(encoded))
	}

	header := http.Header{}
	header.Set("Authorization", os.Getenv("ASSEMBLYAI_API_KEY"))

	conn, resp, err := websocket.DefaultDialer.Dial(assemblyAIURL+"?"+params.Encode(), header)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial AssemblyAI: %w (HTTP %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial AssemblyAI: %w", err)
	}
	return conn, nil
}

// readTranscripts prints partials in place and finals on their own line.
func readTranscripts(conn *websocket.Conn, done chan<- struct{}) {
	defer close(done)
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				log.Printf("AssemblyAI connection closed: %v", err)
			}
			return
		}
		var msg AssemblyAIMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "Begin":
			log.Printf("AssemblyAI session %s", msg.ID)
		case "Turn":
			text := strings.TrimSpace(msg.Transcript)
			if text == "" {
				continue // 3.7 Realtime can send an empty final after background noise
			}
			if msg.EndOfTurn {
				fmt.Printf("\r\033[K%s\n", text) // final: keep it
			} else {
				fmt.Printf("\r\033[K%s", text) // partial: replaces the previous one
			}
		case "Termination":
			log.Printf("Session ended: %.1f s of audio processed", msg.AudioDurationSeconds)
			return
		case "Error":
			log.Printf("AssemblyAI error %d: %s", msg.ErrorCode, msg.Error)
		}
	}
}

// mediaHandler receives one call's audio from Twilio and forwards it.
func mediaHandler(w http.ResponseWriter, r *http.Request) {
	twilio, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade: %v", err)
		return
	}
	defer twilio.Close()

	var (
		aai    *websocket.Conn
		done   chan struct{}
		buffer []byte
		mu     sync.Mutex // gorilla/websocket allows one concurrent writer
	)

	send := func(messageType int, data []byte) {
		mu.Lock()
		defer mu.Unlock()
		if aai != nil {
			if err := aai.WriteMessage(messageType, data); err != nil {
				log.Printf("send to AssemblyAI: %v", err)
			}
		}
	}

	finish := func() {
		if aai == nil {
			return
		}
		if len(buffer) > 0 {
			send(websocket.BinaryMessage, buffer)
			buffer = nil
		}
		send(websocket.TextMessage, []byte(`{"type":"Terminate"}`))
		<-done // wait for the Termination message
		aai.Close()
		aai = nil
	}
	defer finish()

	for {
		_, data, err := twilio.ReadMessage()
		if err != nil {
			return
		}
		var msg TwilioMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		switch msg.Event {
		case "start":
			f := msg.Start.MediaFormat
			log.Printf("Call %s started (%s, %d Hz, %d channel)", msg.Start.CallSid, f.Encoding, f.SampleRate, f.Channels)
			conn, err := connectAssemblyAI()
			if err != nil {
				log.Printf("%v", err)
				return
			}
			aai = conn
			done = make(chan struct{})
			go readTranscripts(aai, done)

		case "media":
			if aai == nil || msg.Media == nil {
				continue
			}
			audio, err := base64.StdEncoding.DecodeString(msg.Media.Payload)
			if err != nil {
				continue
			}
			buffer = append(buffer, audio...)
			if len(buffer) >= bufferBytes {
				send(websocket.BinaryMessage, buffer)
				buffer = nil
			}

		case "stop":
			log.Println("Call ended")
			return
		}
	}
}

func main() {
	_ = godotenv.Load()
	if os.Getenv("ASSEMBLYAI_API_KEY") == "" {
		log.Fatal("Set ASSEMBLYAI_API_KEY in your environment or .env file")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/", twimlHandler)
	http.HandleFunc("/media", mediaHandler)

	log.Printf("Listening on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
