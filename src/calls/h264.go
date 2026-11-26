package calls

import (
	"context"
	"errors"
	"sync"
	"time"
)

// NAL unit types used to cut an Annex-B stream into access units.
const (
	nalSliceNonIDR = 1
	nalSliceIDR    = 5
	nalSEI         = 6
	nalSPS         = 7
	nalPPS         = 8
	nalAUD         = 9
)

// SplitAccessUnits cuts an Annex-B H.264 stream into access units (one coded picture
// each, start codes kept), the unit meowcaller's Call.SendVideo expects. A new access
// unit starts at an AUD/SPS/PPS/SEI that follows a slice, or at a slice whose
// first_mb_in_slice is 0 once the current unit already holds a slice.
func SplitAccessUnits(stream []byte) [][]byte {
	nals := splitNALs(stream)
	var units [][]byte
	var cur []byte
	hasSlice := false
	flush := func() {
		if len(cur) > 0 {
			units = append(units, cur)
		}
		cur, hasSlice = nil, false
	}
	for _, nal := range nals {
		body := nal.body
		if len(body) == 0 {
			continue
		}
		switch t := body[0] & 0x1f; t {
		case nalAUD, nalSPS, nalPPS, nalSEI:
			if hasSlice {
				flush()
			}
		case nalSliceNonIDR, nalSliceIDR:
			firstMB := len(body) > 1 && body[1]&0x80 != 0
			if hasSlice && firstMB {
				flush()
			}
			hasSlice = true
		}
		cur = append(cur, nal.raw...)
	}
	flush()
	return units
}

// IsKeyframe reports whether an access unit holds an IDR slice.
func IsKeyframe(au []byte) bool {
	for _, nal := range splitNALs(au) {
		if len(nal.body) > 0 && nal.body[0]&0x1f == nalSliceIDR {
			return true
		}
	}
	return false
}

type nalUnit struct {
	raw  []byte // with its start code
	body []byte // without it
}

func splitNALs(b []byte) []nalUnit {
	var out []nalUnit
	start, prefix := findStartCode(b, 0)
	for start >= 0 {
		next, nextPrefix := findStartCode(b, start+prefix)
		end := len(b)
		if next >= 0 {
			end = next
		}
		out = append(out, nalUnit{raw: b[start:end], body: b[start+prefix : end]})
		start, prefix = next, nextPrefix
	}
	return out
}

// findStartCode returns the index of the next 00 00 01 or 00 00 00 01 at or after from,
// and the start code length, or -1.
func findStartCode(b []byte, from int) (int, int) {
	for i := from; i+2 < len(b); i++ {
		if b[i] != 0 || b[i+1] != 0 {
			continue
		}
		if b[i+2] == 1 {
			if i > from && b[i-1] == 0 {
				return i - 1, 4
			}
			return i, 3
		}
	}
	return -1, 0
}

// VideoLoop plays pre-encoded access units in a loop at a fixed frame rate. The first
// unit must be a keyframe. RequestKeyframe makes the next frame sent the next keyframe
// in the loop, so a receiver that lost the picture recovers within one GOP.
type VideoLoop struct {
	units     [][]byte
	keyframes []int
	frame     time.Duration

	mu      sync.Mutex
	pos     int
	wantKey bool
	sent    int
}

// NewVideoLoop builds a loop over units played at fps frames per second.
func NewVideoLoop(units [][]byte, fps int) (*VideoLoop, error) {
	if len(units) == 0 {
		return nil, errors.New("calls: empty video")
	}
	if fps <= 0 {
		return nil, errors.New("calls: fps must be positive")
	}
	l := &VideoLoop{units: units, frame: time.Second / time.Duration(fps)}
	for i, u := range units {
		if IsKeyframe(u) {
			l.keyframes = append(l.keyframes, i)
		}
	}
	if len(l.keyframes) == 0 || l.keyframes[0] != 0 {
		return nil, errors.New("calls: video must start with a keyframe")
	}
	return l, nil
}

// FrameDuration is the time each access unit covers.
func (l *VideoLoop) FrameDuration() time.Duration { return l.frame }

// RequestKeyframe jumps to the next keyframe before the following send.
func (l *VideoLoop) RequestKeyframe() {
	l.mu.Lock()
	l.wantKey = true
	l.mu.Unlock()
}

// Next returns the access unit to send now and advances the loop.
func (l *VideoLoop) Next() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.wantKey {
		l.wantKey = false
		l.pos = l.nextKeyframe(l.pos)
	}
	au := l.units[l.pos]
	l.pos = (l.pos + 1) % len(l.units)
	l.sent++
	return au
}

// Sent returns how many access units Next has handed out.
func (l *VideoLoop) Sent() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sent
}

func (l *VideoLoop) nextKeyframe(pos int) int {
	for _, k := range l.keyframes {
		if k >= pos {
			return k
		}
	}
	return l.keyframes[0]
}

// Run sends one access unit per frame interval through send until ctx ends. Send
// errors (no video media yet, call ending) are reported to onErr and the loop goes on.
func (l *VideoLoop) Run(ctx context.Context, send func(au []byte, d time.Duration) error, onErr func(error)) {
	ticker := time.NewTicker(l.frame)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := send(l.Next(), l.frame); err != nil && onErr != nil {
				onErr(err)
			}
		}
	}
}

// VideoCounter counts received access units and keyframes, forwarding each unit to
// an optional next sink function.
type VideoCounter struct {
	mu        sync.Mutex
	units     int
	keyframes int
	bytes     int
}

// Observe records one received access unit.
func (c *VideoCounter) Observe(au []byte) {
	key := IsKeyframe(au)
	c.mu.Lock()
	c.units++
	c.bytes += len(au)
	if key {
		c.keyframes++
	}
	c.mu.Unlock()
}

// Stats returns access units, keyframes and total bytes seen.
func (c *VideoCounter) Stats() (units, keyframes, bytes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.units, c.keyframes, c.bytes
}
