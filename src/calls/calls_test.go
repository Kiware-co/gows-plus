package calls

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/purpshell/meowcaller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
)

const (
	allowedPhone = "573001112233"
	otherPhone   = "573009998877"
)

func TestNormalizePhone(t *testing.T) {
	for in, want := range map[string]string{
		"+57 300 111-2233":   allowedPhone,
		"573001112233":       allowedPhone,
		" (57) 300.111.2233": allowedPhone,
	} {
		got, err := NormalizePhone(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"", "12345", "57300abc2233", "57+3001112233", "1234567890123456"} {
		_, err := NormalizePhone(bad)
		assert.Error(t, err, bad)
	}
}

func TestAllowlist(t *testing.T) {
	list, err := ParseAllowlist(" +57 300 111 2233 , ,573001112233")
	require.NoError(t, err)
	assert.Equal(t, 1, list.Len())
	assert.True(t, list.Allows("+573001112233"))
	assert.False(t, list.Allows(otherPhone))
	assert.False(t, list.Allows("garbage"))

	var zero Allowlist
	assert.False(t, zero.Allows(allowedPhone))

	_, err = ParseAllowlist("573001112233,abc")
	assert.Error(t, err)
}

func TestOutboundTarget(t *testing.T) {
	list, _ := ParseAllowlist(allowedPhone)
	got, err := OutboundTarget(list, "+57 300 111 2233")
	require.NoError(t, err)
	assert.Equal(t, "+"+allowedPhone, got)

	_, err = OutboundTarget(list, otherPhone)
	assert.ErrorIs(t, err, ErrNotAllowed)
}

func TestOfferGate(t *testing.T) {
	list, _ := ParseAllowlist(allowedPhone)
	lids := map[string]string{"111": allowedPhone, "222": otherPhone}
	resolve := func(_ context.Context, lid types.JID) (types.JID, error) {
		assert.Zero(t, lid.Device, "gate must resolve the non-AD LID")
		if pn, ok := lids[lid.User]; ok {
			return types.NewJID(pn, types.DefaultUserServer), nil
		}
		return types.EmptyJID, errors.New("unknown lid")
	}
	lid := func(user string) types.JID {
		return types.JID{User: user, Server: types.HiddenUserServer, Device: 7}
	}

	cases := []struct {
		name   string
		offer  meowcaller.Offer
		allow  bool
		reason string
	}{
		{"allowed lid", meowcaller.Offer{Creator: lid("111"), From: lid("111")}, true, "allowlisted"},
		{"allowed phone jid", meowcaller.Offer{Creator: types.NewJID(allowedPhone, types.DefaultUserServer)}, true, "allowlisted"},
		{"other lid", meowcaller.Offer{Creator: lid("222"), From: lid("222")}, false, "not allowlisted"},
		{"unknown lid", meowcaller.Offer{Creator: lid("333"), From: lid("333")}, false, "caller phone unknown"},
		{"group", meowcaller.Offer{Creator: lid("111"), Group: true}, false, "group call"},
	}
	for _, tc := range cases {
		var seen GateDecision
		gate := NewOfferGate(list, resolve, func(d GateDecision) { seen = d })
		assert.Equal(t, tc.allow, gate(tc.offer), tc.name)
		assert.Equal(t, tc.reason, seen.Reason, tc.name)
	}

	empty := NewOfferGate(Allowlist{}, resolve, nil)
	assert.False(t, empty(meowcaller.Offer{Creator: lid("111")}))
}

func frame(v float32) []float32 {
	f := make([]float32, meowcaller.FrameSamples)
	f[0] = v
	return f
}

func TestEchoDelaysAndReplaysInOrder(t *testing.T) {
	e := NewEcho(2, 4)
	got, err := e.ReadFrame()
	require.NoError(t, err)
	assert.Nil(t, got, "silence before anything arrives")

	require.NoError(t, e.WriteFrame(frame(0.1)))
	got, _ = e.ReadFrame()
	assert.Nil(t, got, "still priming")

	require.NoError(t, e.WriteFrame(frame(0.2)))
	got, _ = e.ReadFrame()
	assert.Equal(t, float32(0.1), got[0])
	got, _ = e.ReadFrame()
	assert.Equal(t, float32(0.2), got[0])
	got, _ = e.ReadFrame()
	assert.Nil(t, got, "underrun is silence, not a stall")

	for i := 0; i < 6; i++ {
		require.NoError(t, e.WriteFrame(frame(float32(i))))
	}
	assert.Equal(t, 2, e.Dropped())
	got, _ = e.ReadFrame()
	assert.Equal(t, float32(2), got[0], "oldest frames dropped when full")

	require.NoError(t, e.Close())
	_, err = e.ReadFrame()
	assert.Error(t, err)
}

func TestEchoCopiesFrames(t *testing.T) {
	e := NewEcho(0, 2)
	f := frame(0.5)
	require.NoError(t, e.WriteFrame(f))
	f[0] = 0
	got, _ := e.ReadFrame()
	assert.Equal(t, float32(0.5), got[0])
}

type countSink struct{ n, closed int }

func (c *countSink) WriteFrame([]float32) error { c.n++; return nil }
func (c *countSink) Close() error               { c.closed++; return nil }

func TestTee(t *testing.T) {
	a, b := &countSink{}, &countSink{}
	tee := NewTee(a, nil, b)
	require.NoError(t, tee.WriteFrame(frame(-0.75)))
	require.NoError(t, tee.WriteFrame(frame(0.25)))
	n, peak := tee.Stats()
	assert.Equal(t, 2, n)
	assert.Equal(t, float32(0.75), peak)
	assert.Equal(t, 2, a.n)
	assert.Equal(t, 2, b.n)
	require.NoError(t, tee.Close())
	assert.Equal(t, 1, a.closed)
}

func TestEchoRecutsLongBlocks(t *testing.T) {
	e := NewEcho(0, 10)
	block := make([]float32, 3*meowcaller.FrameSamples+100) // a 0x92 container decodes to several frames
	for i := range block {
		block[i] = float32(i)
	}
	require.NoError(t, e.WriteFrame(block))
	for i := 0; i < 3; i++ {
		got, err := e.ReadFrame()
		require.NoError(t, err)
		require.Len(t, got, meowcaller.FrameSamples, "every frame the encoder sees is exactly 960 samples")
		assert.Equal(t, float32(i*meowcaller.FrameSamples), got[0])
	}
	got, _ := e.ReadFrame()
	assert.Nil(t, got, "the 100 leftover samples wait for more")
	require.NoError(t, e.WriteFrame(make([]float32, meowcaller.FrameSamples-100)))
	got, _ = e.ReadFrame()
	require.Len(t, got, meowcaller.FrameSamples)
	assert.Equal(t, float32(3*meowcaller.FrameSamples), got[0])
	assert.Equal(t, 4, e.Played())
}

type sliceSource struct{ frames [][]float32 }

func (s *sliceSource) ReadFrame() ([]float32, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	f := s.frames[0]
	s.frames = s.frames[1:]
	return f, nil
}
func (s *sliceSource) Close() error { return nil }

func TestLoop(t *testing.T) {
	l, err := NewLoop(&sliceSource{frames: [][]float32{frame(0.1), frame(0.2)}})
	require.NoError(t, err)
	for _, want := range []float32{0.1, 0.2, 0.1} {
		got, err := l.ReadFrame()
		require.NoError(t, err)
		require.Len(t, got, meowcaller.FrameSamples)
		assert.Equal(t, want, got[0])
	}
	assert.Equal(t, 3, l.Played())
	_, err = NewLoop(&sliceSource{})
	assert.Error(t, err)
}
