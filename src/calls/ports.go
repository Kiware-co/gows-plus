package calls

import (
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
)

// portQueue bounds what a slow client may lag behind before old data is dropped.
const portQueue = 32

// AudioStats are the counters reported to the audio client.
type AudioStats struct {
	InFrames  int     `json:"inFrames"`
	OutFrames int     `json:"outFrames"`
	InPeak    float32 `json:"inPeak"`
}

// AudioPort is the single client attached to a call's audio. Frames carries the
// peer's audio as s16le PCM, exactly FrameBytes per message; Write takes the agent's
// PCM in any even length.
type AudioPort struct {
	s      *session
	frames chan []byte
	framer *Framer
	done   chan struct{}
	once   sync.Once
}

func (s *session) attachAudio() (*AudioPort, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return nil, ErrNotFound
	}
	if s.audio != nil {
		return nil, ErrAttached
	}
	p := &AudioPort{s: s, frames: make(chan []byte, portQueue), framer: NewFramer(portQueue), done: make(chan struct{})}
	s.audio = p
	return p, nil
}

// Frames delivers the peer's audio, one FrameBytes message per 60 ms frame.
func (p *AudioPort) Frames() <-chan []byte { return p.frames }

// Done closes when the call ends or the port is detached.
func (p *AudioPort) Done() <-chan struct{} { return p.done }

// Write queues agent audio (s16le, 16 kHz, mono) towards WhatsApp.
func (p *AudioPort) Write(pcm []byte) {
	p.s.out.Push(PCM16ToFloat(pcm))
}

// Flush drops agent audio still queued.
func (p *AudioPort) Flush() { p.s.out.Reset() }

// Stats returns the call's audio counters.
func (p *AudioPort) Stats() AudioStats { return p.s.stats() }

// Detach releases the port so another client may attach.
func (p *AudioPort) Detach() {
	p.s.mu.Lock()
	if p.s.audio == p {
		p.s.audio = nil
	}
	p.s.mu.Unlock()
	p.close()
}

func (p *AudioPort) close() { p.once.Do(func() { close(p.done) }) }

// deliver re-cuts a decoded block into FrameBytes messages; a full queue drops the
// oldest message so the client hears the present, not the past.
func (p *AudioPort) deliver(block []float32) {
	p.framer.Push(block)
	for f := p.framer.Pop(); f != nil; f = p.framer.Pop() {
		msg := FloatToPCM16(f)
		select {
		case <-p.done:
			return
		default:
		}
		select {
		case p.frames <- msg:
		default:
			select {
			case <-p.frames:
			default:
			}
			select {
			case p.frames <- msg:
			default:
			}
		}
	}
}

// VideoFrame is one H.264 access unit (Annex-B) from the peer.
type VideoFrame struct {
	AccessUnit  []byte
	Keyframe    bool
	Orientation int
}

// VideoControl is an out-of-band video notice for the client.
type VideoControl struct {
	Type        string `json:"type"` // keyframe_request | peer_video
	Active      bool   `json:"active,omitempty"`
	Orientation int    `json:"orientation,omitempty"`
}

// VideoPort is the single client attached to a call's video.
type VideoPort struct {
	s        *session
	frames   chan VideoFrame
	controls chan VideoControl
	done     chan struct{}
	once     sync.Once
}

func (s *session) attachVideo() (*VideoPort, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return nil, ErrNotFound
	}
	if s.video != nil {
		return nil, ErrAttached
	}
	p := &VideoPort{s: s, frames: make(chan VideoFrame, portQueue), controls: make(chan VideoControl, 8), done: make(chan struct{})}
	s.video = p
	return p, nil
}

// Frames delivers the peer's access units.
func (p *VideoPort) Frames() <-chan VideoFrame { return p.frames }

// Controls delivers keyframe requests and peer video state.
func (p *VideoPort) Controls() <-chan VideoControl { return p.controls }

// Done closes when the call ends or the port is detached.
func (p *VideoPort) Done() <-chan struct{} { return p.done }

// Send forwards one of our access units; it is dropped while our camera is off.
func (p *VideoPort) Send(accessUnit []byte, duration time.Duration) error {
	if duration <= 0 {
		duration = time.Second / 15
	}
	return p.s.sendVideo(accessUnit, duration)
}

// SetOrientation announces our camera rotation.
func (p *VideoPort) SetOrientation(orientation int) error {
	return p.s.call.SetVideoOrientation(orientation)
}

// Detach releases the port so another client may attach.
func (p *VideoPort) Detach() {
	p.s.mu.Lock()
	if p.s.video == p {
		p.s.video = nil
	}
	p.s.mu.Unlock()
	p.close()
}

func (p *VideoPort) close() { p.once.Do(func() { close(p.done) }) }

func (p *VideoPort) deliver(f VideoFrame) {
	select {
	case <-p.done:
		return
	case p.frames <- f:
	default:
		// A slow client loses frames; ask it to resync from the next keyframe.
		p.control(VideoControl{Type: "frames_dropped"})
	}
}

func (p *VideoPort) control(c VideoControl) {
	select {
	case <-p.done:
	case p.controls <- c:
	default:
	}
}

var _ meowcaller.AudioSource = sessionSource{}
