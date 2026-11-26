package calls

import (
	"encoding/binary"
	"math"
	"sync"

	"github.com/purpshell/meowcaller"
)

// FrameBytes is one codec frame as 16-bit little-endian PCM: 960 samples, 60 ms.
const FrameBytes = meowcaller.FrameSamples * 2

// Framer joins PCM of any length and cuts it into exact FrameSamples frames, the
// only size the MLow encoder accepts. Frames beyond max are dropped oldest-first,
// so a producer running ahead adds at most max×60 ms of delay.
type Framer struct {
	mu      sync.Mutex
	partial []float32
	frames  [][]float32
	max     int
	dropped int
}

// NewFramer keeps at most maxFrames complete frames queued.
func NewFramer(maxFrames int) *Framer {
	if maxFrames < 1 {
		maxFrames = 1
	}
	return &Framer{max: maxFrames}
}

// Push appends samples and queues every complete frame.
func (f *Framer) Push(samples []float32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partial = append(f.partial, samples...)
	for len(f.partial) >= meowcaller.FrameSamples {
		frame := make([]float32, meowcaller.FrameSamples)
		copy(frame, f.partial)
		f.partial = f.partial[meowcaller.FrameSamples:]
		if len(f.frames) >= f.max {
			f.frames = f.frames[1:]
			f.dropped++
		}
		f.frames = append(f.frames, frame)
	}
	if len(f.partial) == 0 {
		f.partial = nil
	}
}

// Pop returns the oldest complete frame, or nil when none is ready.
func (f *Framer) Pop() []float32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.frames) == 0 {
		return nil
	}
	frame := f.frames[0]
	f.frames = f.frames[1:]
	return frame
}

// Reset discards everything queued.
func (f *Framer) Reset() {
	f.mu.Lock()
	f.frames, f.partial = nil, nil
	f.mu.Unlock()
}

// Queued returns how many complete frames wait.
func (f *Framer) Queued() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.frames)
}

// Dropped returns how many frames were discarded for overflow.
func (f *Framer) Dropped() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dropped
}

// PCM16ToFloat converts s16le bytes to samples in [-1, 1). A trailing odd byte is ignored.
func PCM16ToFloat(b []byte) []float32 {
	out := make([]float32, len(b)/2)
	for i := range out {
		out[i] = float32(int16(binary.LittleEndian.Uint16(b[2*i:]))) / 32768
	}
	return out
}

// FloatToPCM16 converts samples to s16le bytes, clipping to [-1, 1].
func FloatToPCM16(samples []float32) []byte {
	out := make([]byte, len(samples)*2)
	for i, s := range samples {
		v := math.Round(float64(s) * 32767)
		if v > 32767 {
			v = 32767
		} else if v < -32768 {
			v = -32768
		}
		binary.LittleEndian.PutUint16(out[2*i:], uint16(int16(v)))
	}
	return out
}
