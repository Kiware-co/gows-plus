package calls

import (
	"errors"
	"io"
	"sync"

	"github.com/purpshell/meowcaller"
)

// Echo plays the peer's own voice back to them after a delay. It is both the sink
// that receives the peer's frames and the source the call plays from, which makes
// it a self-contained two-way audio test: the caller hears themselves.
//
// Peer audio can arrive in blocks longer than one codec frame (video calls pack several
// 60 ms frames in one packet), so Echo re-cuts it into exact FrameSamples frames: the
// encoder rejects any other size and the engine then sends nothing for that tick.
//
// ReadFrame never blocks: with nothing queued it returns a nil frame, which the
// engine sends as silence.
type Echo struct {
	mu      sync.Mutex
	delay   int
	queue   [][]float32
	partial []float32
	limit   int
	primed  bool
	closed  bool
	dropped int
	played  int
}

// NewEcho returns an Echo that starts replaying once delayFrames frames
// (60 ms each) are queued, keeping at most maxFrames queued.
func NewEcho(delayFrames, maxFrames int) *Echo {
	if delayFrames < 0 {
		delayFrames = 0
	}
	if maxFrames <= delayFrames {
		maxFrames = delayFrames + 1
	}
	return &Echo{delay: delayFrames, limit: maxFrames}
}

// WriteFrame queues the peer's samples as FrameSamples-long frames, dropping the oldest
// frames when full. Samples that do not fill a frame wait for the next write.
func (e *Echo) WriteFrame(frame []float32) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.partial = append(e.partial, frame...)
	for len(e.partial) >= meowcaller.FrameSamples {
		chunk := make([]float32, meowcaller.FrameSamples)
		copy(chunk, e.partial)
		e.partial = e.partial[meowcaller.FrameSamples:]
		if len(e.queue) >= e.limit {
			e.queue = e.queue[1:]
			e.dropped++
		}
		e.queue = append(e.queue, chunk)
	}
	if len(e.partial) == 0 {
		e.partial = nil
	}
	return nil
}

// ReadFrame returns the next delayed frame, or nil (silence) when none is ready.
func (e *Echo) ReadFrame() ([]float32, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, errEchoClosed
	}
	if !e.primed {
		if len(e.queue) < e.delay || len(e.queue) == 0 {
			return nil, nil
		}
		e.primed = true
	}
	if len(e.queue) == 0 {
		return nil, nil
	}
	frame := e.queue[0]
	e.queue = e.queue[1:]
	e.played++
	return frame, nil
}

// Played reports how many voice frames were handed to the call (not counting silence).
func (e *Echo) Played() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.played
}

// Dropped reports how many frames were discarded because the queue was full.
func (e *Echo) Dropped() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// Close stops the echo. Safe to call more than once.
func (e *Echo) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	e.queue = nil
	e.partial = nil
	return nil
}

var errEchoClosed = errors.New("calls: echo closed")

// Tee fans one stream of peer frames out to several sinks and counts the frames
// and the loudest sample seen, so a test can tell real voice from silence.
type Tee struct {
	sinks []meowcaller.AudioSink

	mu     sync.Mutex
	frames int
	peak   float32
}

// NewTee returns a sink writing every frame to each of sinks (nil entries skipped).
func NewTee(sinks ...meowcaller.AudioSink) *Tee {
	t := &Tee{}
	for _, s := range sinks {
		if s != nil {
			t.sinks = append(t.sinks, s)
		}
	}
	return t
}

// WriteFrame forwards frame to every sink, returning the first error.
func (t *Tee) WriteFrame(frame []float32) error {
	t.mu.Lock()
	t.frames++
	for _, v := range frame {
		if v < 0 {
			v = -v
		}
		if v > t.peak {
			t.peak = v
		}
	}
	t.mu.Unlock()
	var first error
	for _, s := range t.sinks {
		if err := s.WriteFrame(frame); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Stats returns how many frames passed and the peak absolute sample (0..1).
func (t *Tee) Stats() (frames int, peak float32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.frames, t.peak
}

// Close closes every sink, returning the first error.
func (t *Tee) Close() error {
	var first error
	for _, s := range t.sinks {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Loop replays a decoded audio clip forever as FrameSamples frames. Used to isolate the
// outbound audio path from what the peer sends.
type Loop struct {
	mu     sync.Mutex
	frames [][]float32
	pos    int
	played int
}

// NewLoop reads src to the end and loops its frames. src is closed.
func NewLoop(src meowcaller.AudioSource) (*Loop, error) {
	defer src.Close()
	var all []float32
	for {
		f, err := src.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		all = append(all, f...)
	}
	l := &Loop{}
	for i := 0; i+meowcaller.FrameSamples <= len(all); i += meowcaller.FrameSamples {
		l.frames = append(l.frames, all[i:i+meowcaller.FrameSamples])
	}
	if len(l.frames) == 0 {
		return nil, errors.New("calls: audio clip shorter than one frame")
	}
	return l, nil
}

// ReadFrame returns the next frame of the clip, wrapping around.
func (l *Loop) ReadFrame() ([]float32, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f := l.frames[l.pos]
	l.pos = (l.pos + 1) % len(l.frames)
	l.played++
	return f, nil
}

// Played reports how many frames were handed out.
func (l *Loop) Played() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.played
}

// Close is a no-op; the loop never ends on its own.
func (l *Loop) Close() error { return nil }
