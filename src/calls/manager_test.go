package calls

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// fakeCall records what the manager does to a call and lets tests fire callbacks.
type fakeCall struct {
	mu      sync.Mutex
	id      string
	peer    types.JID
	video   bool
	answers int
	rejects int
	hangups int
	sent    [][]byte
	player  *meowcaller.Player
	src     meowcaller.AudioSource
	sink    meowcaller.AudioSink
	vsink   meowcaller.VideoSink
	onEnd   func(string)
	onReady func()
	onVideo func(meowcaller.VideoState)
	onKey   func()
	camera  []bool
	upgrade int
	accepts int
}

func (f *fakeCall) ID() string                                  { return f.id }
func (f *fakeCall) Peer() types.JID                             { return f.peer }
func (f *fakeCall) IsVideo() bool                               { return f.video }
func (f *fakeCall) State() meowcaller.CallPhase                 { return meowcaller.CallPhaseRinging }
func (f *fakeCall) Answer() error                               { f.mu.Lock(); f.answers++; f.mu.Unlock(); return nil }
func (f *fakeCall) Reject() error                               { f.rejects++; go f.onEnd("rejected"); return nil }
func (f *fakeCall) Hangup() error                               { f.hangups++; go f.onEnd("hangup"); return nil }
func (f *fakeCall) Receive(s meowcaller.AudioSink)              { f.sink = s }
func (f *fakeCall) ReceiveVideo(s meowcaller.VideoSink)         { f.vsink = s }
func (f *fakeCall) SetVideoEnabled(on bool) error               { f.camera = append(f.camera, on); return nil }
func (f *fakeCall) StartVideo() error                           { f.upgrade++; return nil }
func (f *fakeCall) AcceptVideo() error                          { f.accepts++; return nil }
func (f *fakeCall) SetVideoOrientation(int) error               { return nil }
func (f *fakeCall) OnReady(fn func())                           { f.onReady = fn }
func (f *fakeCall) OnEnd(fn func(string))                       { f.onEnd = fn }
func (f *fakeCall) OnStateChange(func(meowcaller.CallPhase))    {}
func (f *fakeCall) OnVideoState(fn func(meowcaller.VideoState)) { f.onVideo = fn }
func (f *fakeCall) OnVideoKeyframeRequest(fn func())            { f.onKey = fn }
func (f *fakeCall) Play(src meowcaller.AudioSource) *meowcaller.Player {
	f.src = src
	return nil
}
func (f *fakeCall) SendVideoWithDuration(au []byte, _ time.Duration) error {
	f.sent = append(f.sent, au)
	return nil
}

type recorder struct {
	mu     sync.Mutex
	events []any
}

func (r *recorder) emit(e any) { r.mu.Lock(); r.events = append(r.events, e); r.mu.Unlock() }

func (r *recorder) last() any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) == 0 {
		return nil
	}
	return r.events[len(r.events)-1]
}

func (r *recorder) ended(t *testing.T) *Ended {
	t.Helper()
	var got *Ended
	require.Eventually(t, func() bool {
		e, ok := r.last().(*Ended)
		got = e
		return ok
	}, time.Second, 5*time.Millisecond)
	return got
}

func testManager(t *testing.T) (*Manager, *recorder) {
	t.Helper()
	rec := &recorder{}
	m := newManager(zerolog.Nop(), rec.emit)
	m.resolve = func(_ context.Context, lid types.JID) (types.JID, error) {
		return types.NewJID(allowedPhone, types.DefaultUserServer), nil
	}
	m.ownUser = func() []string { return []string{"573000000000", "999"} }
	_, err := m.SetSettings(true, []string{allowedPhone})
	require.NoError(t, err)
	return m, rec
}

func incoming(m *Manager, id string, video bool) (*fakeCall, *session) {
	c := &fakeCall{id: id, peer: types.JID{User: "111", Server: types.HiddenUserServer}, video: video}
	return c, m.track(c, DirectionIncoming)
}

func TestGateFollowsSettingsAndBusy(t *testing.T) {
	m, _ := testManager(t)
	lid := types.JID{User: "111", Server: types.HiddenUserServer}
	offer := meowcaller.Offer{CallID: "A", From: lid, Creator: lid}
	assert.True(t, m.gate(offer))

	_, err := m.SetSettings(false, []string{allowedPhone})
	require.NoError(t, err)
	assert.False(t, m.gate(offer), "calls off: never engage")

	_, _ = m.SetSettings(true, []string{otherPhone})
	assert.False(t, m.gate(offer), "number not allowed")

	_, _ = m.SetSettings(true, []string{"*"})
	assert.True(t, m.gate(offer), "everyone allowed")
	assert.Equal(t, []string{"*"}, m.Settings().Allow)

	incoming(m, "A", false)
	assert.False(t, m.gate(meowcaller.Offer{CallID: "B", From: lid, Creator: lid}), "busy with another call")

	m.hookErr = assert.AnError
	assert.False(t, m.gate(offer), "hook failed: calls stay off")
	assert.ErrorIs(t, m.Available(), ErrUnavailable)
	assert.False(t, m.Settings().Available)
}

func TestIncomingCallLifecycle(t *testing.T) {
	m, rec := testManager(t)
	c, s := incoming(m, "CALL1", false)
	require.NotNil(t, s)

	ringing, ok := rec.last().(*Ringing)
	require.True(t, ok)
	assert.Equal(t, StateRinging, ringing.State)
	assert.Equal(t, allowedPhone, ringing.Phone)
	assert.Equal(t, allowedPhone+"@c.us", ringing.ChatID)

	info, err := m.Answer("CALL1", "agent-1")
	require.NoError(t, err)
	assert.Equal(t, StateConnecting, info.State)

	info, err = m.Answer("CALL1", "agent-2")
	assert.ErrorIs(t, err, ErrAlreadyAnswered, "first answer wins")
	assert.Equal(t, "agent-1", info.AnsweredBy)
	assert.Equal(t, 1, c.answers)

	c.onReady()
	conn, ok := rec.last().(*Connected)
	require.True(t, ok)
	assert.Equal(t, StateActive, conn.State)

	c.onEnd("")
	ended := rec.ended(t)
	assert.Equal(t, ReasonHangupRemote, ended.Reason)
	assert.Empty(t, m.List(), "slot freed")
	_, err = m.Get("CALL1")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestConcurrentAnswersOnlyOneWins(t *testing.T) {
	m, _ := testManager(t)
	c, _ := incoming(m, "RACE", false)
	var wg sync.WaitGroup
	wins := make(chan string, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := m.Answer("RACE", string(rune('a'+i))); err == nil {
				wins <- "win"
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	assert.Len(t, wins, 1)
	assert.Equal(t, 1, c.answers)
}

func TestLocalEndReasons(t *testing.T) {
	m, rec := testManager(t)
	_, _ = incoming(m, "R", false)
	require.NoError(t, m.Reject("R"))
	assert.Equal(t, ReasonRejectedLocal, rec.ended(t).Reason)

	c, _ := incoming(m, "H", false)
	_, _ = m.Answer("H", "a")
	c.onReady()
	require.NoError(t, m.Hangup("H"))
	assert.Equal(t, ReasonHangupLocal, rec.ended(t).Reason)

	_, _ = incoming(m, "X", false)
	_, err := m.Answer("X", "a")
	require.NoError(t, err)
	assert.ErrorIs(t, m.Reject("X"), ErrWrongState, "cannot reject an answered call")
}

func TestAnsweredOnAnotherDevice(t *testing.T) {
	m, rec := testManager(t)
	_, _ = incoming(m, "E", false)
	m.onWhatsmeowEvent(&events.CallAccept{BasicCallMeta: types.BasicCallMeta{
		From: types.JID{User: "999", Device: 3, Server: types.HiddenUserServer}, CallID: "E",
	}})
	assert.Equal(t, ReasonAnsweredElsewhere, rec.ended(t).Reason)
	assert.Empty(t, m.List())
}

func TestEndReasonMapping(t *testing.T) {
	cases := []struct {
		raw   string
		facts endFacts
		want  string
	}{
		{"", endFacts{connected: true}, ReasonHangupRemote},
		{"", endFacts{}, ReasonMissed},
		{"timeout", endFacts{}, ReasonMissed},
		{"rejected", endFacts{}, ReasonRejectedRemote},
		{"busy", endFacts{}, ReasonBusy},
		{"server:500", endFacts{connected: true}, ReasonFailed},
		{"hangup", endFacts{localHangup: true, connected: true}, ReasonHangupLocal},
		{"rejected", endFacts{localReject: true}, ReasonRejectedLocal},
		{"", endFacts{elsewhere: true}, ReasonAnsweredElsewhere},
		{"accepted_elsewhere", endFacts{}, ReasonAnsweredElsewhere},
		{"rejected_elsewhere", endFacts{}, ReasonRejectedElsewhere},
	}
	for _, tc := range cases {
		got, _ := endReason(tc.raw, tc.facts)
		assert.Equal(t, tc.want, got, "%q %+v", tc.raw, tc.facts)
	}
}

func TestSilenceDetection(t *testing.T) {
	m, rec := testManager(t)
	clock := time.Unix(1000, 0)
	m.now = func() time.Time { return clock }
	c, s := incoming(m, "S", false)
	_, _ = m.Answer("S", "a")
	c.onReady()

	clock = clock.Add(4 * time.Second)
	s.checkSilence(clock)
	_, isSilent := rec.last().(*AudioSilent)
	assert.False(t, isSilent)

	clock = clock.Add(2 * time.Second)
	s.checkSilence(clock)
	silent, ok := rec.last().(*AudioSilent)
	require.True(t, ok)
	assert.Equal(t, int64(6000), silent.SilentMs)

	require.NoError(t, c.sink.WriteFrame(make([]float32, meowcaller.FrameSamples)))
	s.checkSilence(clock)
	_, restored := rec.last().(*AudioRestored)
	assert.True(t, restored)
}

func TestAudioPortCutsBothWays(t *testing.T) {
	m, _ := testManager(t)
	c, _ := incoming(m, "AU", false)
	port, err := m.AttachAudio("AU")
	require.NoError(t, err)
	_, err = m.AttachAudio("AU")
	assert.ErrorIs(t, err, ErrAttached, "one client per call")

	// Peer: a 3-frame block (video-call container) comes out as three 1920-byte messages.
	require.NoError(t, c.sink.WriteFrame(make([]float32, 3*meowcaller.FrameSamples)))
	for i := 0; i < 3; i++ {
		select {
		case msg := <-port.Frames():
			assert.Len(t, msg, FrameBytes)
		case <-time.After(time.Second):
			t.Fatal("missing frame")
		}
	}

	// Agent: odd-sized writes reach the call as exact 960-sample frames.
	port.Write(make([]byte, 1000))
	got, _ := c.src.ReadFrame()
	assert.Nil(t, got, "500 samples are not a frame yet")
	port.Write(make([]byte, 3000))
	got, _ = c.src.ReadFrame()
	assert.Len(t, got, meowcaller.FrameSamples)
	assert.Equal(t, 1, port.Stats().OutFrames)

	_, _ = m.Mute("AU", true)
	port.Write(make([]byte, FrameBytes))
	got, _ = c.src.ReadFrame()
	assert.Nil(t, got, "muted sends silence")

	port.Detach()
	_, err = m.AttachAudio("AU")
	assert.NoError(t, err, "a detached slot can be taken again")
}

func TestAudioPortClosesWhenCallEnds(t *testing.T) {
	m, _ := testManager(t)
	c, _ := incoming(m, "D", false)
	port, err := m.AttachAudio("D")
	require.NoError(t, err)
	c.onEnd("")
	select {
	case <-port.Done():
	case <-time.After(time.Second):
		t.Fatal("port not closed at call end")
	}
}

func TestVideoPortAndCamera(t *testing.T) {
	m, rec := testManager(t)
	c, _ := incoming(m, "V", true)
	port, err := m.AttachVideo("V")
	require.NoError(t, err)

	idr := []byte{0, 0, 0, 1, 0x65, 0x88}
	require.NoError(t, c.vsink.WriteVideo(idr))
	f := <-port.Frames()
	assert.True(t, f.Keyframe)

	c.onKey()
	assert.Equal(t, "keyframe_request", (<-port.Controls()).Type)
	c.onVideo(meowcaller.VideoState{Active: true, Orientation: 1})
	assert.Equal(t, VideoControl{Type: "peer_video", Active: true, Orientation: 1}, <-port.Controls())
	ev, ok := rec.last().(*Video)
	require.True(t, ok)
	assert.True(t, ev.PeerVideoActive)

	require.NoError(t, port.Send(idr, 0))
	assert.Len(t, c.sent, 1, "camera on in a video call")
	_, err = m.SetVideo("V", false)
	require.NoError(t, err)
	require.NoError(t, port.Send(idr, 0))
	assert.Len(t, c.sent, 1, "frames dropped while the camera is off")
	assert.Equal(t, []bool{false}, c.camera)
}

func TestVoiceCallUpgrade(t *testing.T) {
	m, _ := testManager(t)
	c, _ := incoming(m, "U", false)
	info, err := m.SetVideo("U", true)
	require.NoError(t, err)
	assert.Equal(t, 1, c.upgrade, "voice call asks the peer to switch to video")
	assert.True(t, info.IsVideo)

	m2, _ := testManager(t)
	c2, _ := incoming(m2, "P", false)
	c2.onVideo(meowcaller.VideoState{Upgrade: true})
	_, err = m2.SetVideo("P", true)
	require.NoError(t, err)
	assert.Equal(t, 1, c2.accepts, "peer's upgrade request is accepted")
	assert.Equal(t, []bool{true}, c2.camera, "and our own camera is turned on with it")
}

func TestSettingsRejectsBadNumbers(t *testing.T) {
	m, _ := testManager(t)
	_, err := m.SetSettings(true, []string{"abc"})
	assert.Error(t, err)
	assert.True(t, m.Settings().Enabled, "previous settings kept")
}

func TestDisabledManagerRefusesStart(t *testing.T) {
	m, _ := testManager(t)
	_, _ = m.SetSettings(false, nil)
	_, err := m.Start(context.Background(), allowedPhone, false)
	assert.ErrorIs(t, err, ErrDisabled)
	_, _ = m.SetSettings(true, []string{allowedPhone})
	_, err = m.Start(context.Background(), otherPhone, false)
	assert.ErrorIs(t, err, ErrNotAllowed)
}

func TestPeerOrientationFromRTP(t *testing.T) {
	m, rec := testManager(t)
	c, _ := incoming(m, "O", true)
	port, err := m.AttachVideo("O")
	require.NoError(t, err)
	sink, ok := c.vsink.(meowcaller.VideoOrientationSink)
	require.True(t, ok, "the sink must take the per-frame rotation")
	sink.SetOrientation(1)
	assert.Equal(t, VideoControl{Type: "peer_video", Orientation: 1}, <-port.Controls())
	ev, ok := rec.last().(*Video)
	require.True(t, ok)
	assert.Equal(t, 1, ev.Orientation)

	idr := []byte{0, 0, 0, 1, 0x65, 0x88}
	require.NoError(t, c.vsink.WriteVideo(idr))
	assert.Equal(t, 1, (<-port.Frames()).Orientation, "frames carry the current rotation")

	sink.SetOrientation(1)
	select {
	case got := <-port.Controls():
		t.Fatalf("unchanged rotation must not be re-sent: %+v", got)
	default:
	}
}
