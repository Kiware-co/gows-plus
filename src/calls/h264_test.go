package calls

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	sc4 = []byte{0, 0, 0, 1}
	sc3 = []byte{0, 0, 1}
)

func nal(sc []byte, header byte, rest ...byte) []byte {
	return append(append(append([]byte{}, sc...), header), rest...)
}

// gop builds SPS PPS IDR(first_mb=0) followed by n P slices.
func gop(n int) []byte {
	var b []byte
	b = append(b, nal(sc4, 0x67, 0x42, 0xc0)...) // SPS
	b = append(b, nal(sc4, 0x68, 0xce)...)       // PPS
	b = append(b, nal(sc3, 0x65, 0x88, 0x84)...) // IDR, first_mb=0
	for i := 0; i < n; i++ {
		b = append(b, nal(sc3, 0x41, 0x9a, byte(i+1))...) // P, first_mb=0
	}
	return b
}

func TestSplitAccessUnits(t *testing.T) {
	stream := append(gop(2), gop(1)...)
	units := SplitAccessUnits(stream)
	require.Len(t, units, 5)
	assert.True(t, IsKeyframe(units[0]))
	assert.True(t, bytes.HasPrefix(units[0], nal(sc4, 0x67)), "SPS/PPS stay with the IDR")
	assert.False(t, IsKeyframe(units[1]))
	assert.False(t, IsKeyframe(units[2]))
	assert.True(t, IsKeyframe(units[3]))
	assert.Equal(t, stream, bytes.Join(units, nil), "nothing lost or reordered")
}

func TestSplitKeepsMultiSliceFrameTogether(t *testing.T) {
	var b []byte
	b = append(b, nal(sc4, 0x65, 0x88)...) // IDR slice, first_mb=0
	b = append(b, nal(sc3, 0x65, 0x12)...) // IDR slice, first_mb!=0 (same picture)
	b = append(b, nal(sc3, 0x41, 0x9a)...) // next picture
	units := SplitAccessUnits(b)
	require.Len(t, units, 2)
}

func TestVideoLoop(t *testing.T) {
	units := SplitAccessUnits(append(gop(3), gop(3)...)) // keyframes at 0 and 4
	loop, err := NewVideoLoop(units, 15)
	require.NoError(t, err)
	assert.Equal(t, time.Second/15, loop.FrameDuration())

	assert.True(t, IsKeyframe(loop.Next()))
	loop.Next()
	loop.RequestKeyframe()
	got := loop.Next()
	assert.Equal(t, units[4], got, "jumps forward to the next keyframe")
	for i := 0; i < 3; i++ {
		loop.Next()
	}
	assert.Equal(t, units[0], loop.Next(), "wraps around")
	assert.Equal(t, 7, loop.Sent())

	_, err = NewVideoLoop(units[1:], 15)
	assert.Error(t, err, "must start with a keyframe")
	_, err = NewVideoLoop(nil, 15)
	assert.Error(t, err)
}

func TestVideoLoopRun(t *testing.T) {
	loop, err := NewVideoLoop(SplitAccessUnits(gop(1)), 100)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	sent, errs := 0, 0
	loop.Run(ctx, func([]byte, time.Duration) error {
		sent++
		if sent == 1 {
			return errors.New("no video media yet")
		}
		return nil
	}, func(error) { errs++ })
	assert.GreaterOrEqual(t, sent, 3)
	assert.Equal(t, 1, errs, "errors are reported and the loop goes on")
}

func TestVideoCounter(t *testing.T) {
	var c VideoCounter
	for _, u := range SplitAccessUnits(gop(2)) {
		c.Observe(u)
	}
	units, keys, n := c.Stats()
	assert.Equal(t, 3, units)
	assert.Equal(t, 1, keys)
	assert.Greater(t, n, 0)
}

// TestSplitRealFile checks a real encoder output when CALLS_H264 points at one.
func TestSplitRealFile(t *testing.T) {
	path := os.Getenv("CALLS_H264")
	if path == "" {
		t.Skip("set CALLS_H264 to an Annex-B file")
	}
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	units := SplitAccessUnits(b)
	keys := 0
	for _, u := range units {
		if IsKeyframe(u) {
			keys++
		}
	}
	t.Logf("access units=%d keyframes=%d", len(units), keys)
	_, err = NewVideoLoop(units, 15)
	require.NoError(t, err)
}
