package calls

import (
	"errors"
	"sync"

	"github.com/purpshell/meowcaller"
)

// Echo plays the peer's own voice back to them after a delay. It is both the sink
// that receives the peer's frames and the source the call plays from, which makes
// it a self-contained two-way audio test: the caller hears themselves.
//
// ReadFrame never blocks: with nothing queued it returns a nil frame, which the
// engine sends as silence.
type Echo struct {
	mu      sync.Mutex
	delay   int
	queue   [][]float32
	limit   int
	primed  bool
	closed  bool
	dropped int
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

// WriteFrame queues a copy of a peer frame, dropping the oldest when full.
func (e *Echo) WriteFrame(frame []float32) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	if len(e.queue) >= e.limit {
		e.queue = e.queue[1:]
		e.dropped++
	}
	e.queue = append(e.queue, append([]float32(nil), frame...))
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
	return frame, nil
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
