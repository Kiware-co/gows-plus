package calls

import (
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types"
)

// session is one managed call.
type session struct {
	m         *Manager
	call      liveCall
	id        string
	direction string
	started   time.Time
	out       *Framer

	mu          sync.Mutex
	info        Info
	answered    bool
	ended       bool
	connectedAt time.Time
	localHangup bool
	localReject bool
	elsewhere   bool
	upgradeAsk  bool
	orientation int

	lastInbound time.Time
	silentSince time.Time
	silent      bool
	inFrames    int
	inPeak      float32
	outFrames   int
	videoIn     int
	videoInKey  int
	videoOut    int
	videoErrs   int

	audio *AudioPort
	video *VideoPort
}

func (s *session) wire() {
	c := s.call
	c.Receive(meowcaller.SinkFunc(s.onPeerAudio))
	c.Play(sessionSource{s})
	c.ReceiveVideo(peerVideoSink{s})
	c.OnReady(s.onConnected)
	c.OnStateChange(func(p meowcaller.CallPhase) {
		if p == meowcaller.CallPhaseActive {
			s.onConnected()
		}
	})
	c.OnEnd(s.onEnd)
	c.OnVideoState(s.onVideoState)
	c.OnVideoKeyframeRequest(s.onKeyframeRequest)
}

func (s *session) snapshot() Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info
}

func (s *session) setPhone(phone string) {
	s.mu.Lock()
	s.info.Phone = phone
	s.info.ChatID = phone + "@" + types.LegacyUserServer
	s.mu.Unlock()
}

func (s *session) answer(agent string) (Info, error) {
	s.mu.Lock()
	switch {
	case s.answered:
		info := s.info
		s.mu.Unlock()
		return info, ErrAlreadyAnswered
	case s.direction != DirectionIncoming || s.ended || s.info.State != StateRinging:
		info := s.info
		s.mu.Unlock()
		return info, ErrWrongState
	}
	s.answered = true
	s.info.AnsweredBy = agent
	s.info.State = StateConnecting
	s.mu.Unlock()
	if err := s.call.Answer(); err != nil {
		s.mu.Lock()
		s.answered = false
		s.info.AnsweredBy = ""
		s.info.State = StateRinging
		s.mu.Unlock()
		return s.snapshot(), err
	}
	s.m.log.Info().Str("call_id", s.id).Str("agent", agent).Msg("call answered")
	return s.snapshot(), nil
}

func (s *session) reject() error {
	s.mu.Lock()
	if s.direction != DirectionIncoming || s.answered || s.ended {
		s.mu.Unlock()
		return ErrWrongState
	}
	s.localReject = true
	s.mu.Unlock()
	return s.call.Reject()
}

func (s *session) hangup() error {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return nil
	}
	s.localHangup = true
	s.mu.Unlock()
	return s.call.Hangup()
}

func (s *session) setMuted(muted bool) Info {
	s.mu.Lock()
	s.info.Muted = muted
	s.mu.Unlock()
	if muted {
		s.out.Reset()
	}
	return s.snapshot()
}

func (s *session) setVideo(enabled bool) (Info, error) {
	s.mu.Lock()
	isVideo, upgradeAsk, current := s.info.IsVideo, s.upgradeAsk, s.info.VideoEnabled
	s.mu.Unlock()
	var err error
	switch {
	// A pending peer request is answered even when our camera is already on: the phone
	// resends it every ~5 s and shows "loading" (freezing both pictures) until accepted.
	case enabled && upgradeAsk:
		// Accepting only lets the peer's camera in; our camera is a separate switch.
		if err = s.call.AcceptVideo(); err == nil {
			err = s.call.SetVideoEnabled(true)
		}
	case enabled == current:
	case enabled && !isVideo:
		err = s.call.StartVideo()
	default:
		err = s.call.SetVideoEnabled(enabled)
	}
	if err != nil {
		return s.snapshot(), err
	}
	s.mu.Lock()
	s.info.VideoEnabled = enabled
	if enabled {
		s.info.IsVideo = true
		s.upgradeAsk = false
	}
	s.mu.Unlock()
	return s.snapshot(), nil
}

func (s *session) onConnected() {
	now := s.m.now()
	s.mu.Lock()
	if s.ended || !s.connectedAt.IsZero() {
		s.mu.Unlock()
		return
	}
	s.connectedAt = now
	s.lastInbound = now
	s.info.State = StateActive
	s.info.ConnectedAt = now.Unix()
	info := s.info
	s.mu.Unlock()
	s.m.log.Info().Str("call_id", s.id).Msg("call connected")
	s.m.emit(&Connected{Info: info})
}

func (s *session) onEnd(raw string) {
	s.finish(raw)
}

func (s *session) answeredElsewhere() {
	s.mu.Lock()
	if s.answered || s.ended {
		s.mu.Unlock()
		return
	}
	s.elsewhere = true
	s.mu.Unlock()
	s.m.log.Info().Str("call_id", s.id).Msg("answered on another device")
	s.finish("")
}

// finish emits Ended once, closes the ports and frees the manager slot.
func (s *session) finish(raw string) {
	now := s.m.now()
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	reason, detail := endReason(raw, endFacts{
		localHangup: s.localHangup,
		localReject: s.localReject,
		elsewhere:   s.elsewhere,
		connected:   !s.connectedAt.IsZero(),
	})
	s.info.State = StateEnded
	ev := &Ended{Info: s.info, Reason: reason, Detail: detail, DurationMs: now.Sub(s.started).Milliseconds()}
	if !s.connectedAt.IsZero() {
		ev.ConnectedMs = now.Sub(s.connectedAt).Milliseconds()
	}
	audio, video := s.audio, s.video
	s.audio, s.video = nil, nil
	stats := [...]int{s.inFrames, s.outFrames, s.videoIn, s.videoInKey, s.videoOut, s.videoErrs}
	s.mu.Unlock()

	if audio != nil {
		audio.close()
	}
	if video != nil {
		video.close()
	}
	s.out.Reset()
	s.m.release(s)
	s.m.log.Info().Str("call_id", s.id).Str("reason", reason).Str("raw_reason", raw).Int64("duration_ms", ev.DurationMs).
		Int("audio_in_frames", stats[0]).Int("audio_out_frames", stats[1]).
		Int("video_in_frames", stats[2]).Int("video_in_keyframes", stats[3]).
		Int("video_out_frames", stats[4]).Int("video_out_errors", stats[5]).
		Msg("call ended")
	s.m.emit(ev)
}

// onPeerAudio receives decoded peer audio (blocks of one or more 60 ms frames).
func (s *session) onPeerAudio(block []float32) {
	now := s.m.now()
	var peak float32
	for _, v := range block {
		if v < 0 {
			v = -v
		}
		if v > peak {
			peak = v
		}
	}
	s.mu.Lock()
	s.lastInbound = now
	s.inFrames += len(block) / meowcaller.FrameSamples
	if peak > s.inPeak {
		s.inPeak = peak
	}
	port := s.audio
	s.mu.Unlock()
	if port != nil {
		port.deliver(block)
	}
}

// checkSilence emits AudioSilent / AudioRestored for a connected call.
func (s *session) checkSilence(now time.Time) {
	s.mu.Lock()
	if s.ended || s.connectedAt.IsZero() {
		s.mu.Unlock()
		return
	}
	gap := now.Sub(s.lastInbound)
	var ev any
	switch {
	case !s.silent && gap >= silenceAfter:
		s.silent = true
		s.silentSince = s.lastInbound
		ev = &AudioSilent{CallID: s.id, SilentMs: gap.Milliseconds()}
	case s.silent && gap < time.Second:
		s.silent = false
		ev = &AudioRestored{CallID: s.id, SilentMs: s.lastInbound.Sub(s.silentSince).Milliseconds()}
	}
	s.mu.Unlock()
	if ev != nil {
		s.m.emit(ev)
	}
}

// nextOutFrame feeds the call's Player: agent audio, or nil (silence).
func (s *session) nextOutFrame() []float32 {
	s.mu.Lock()
	muted := s.info.Muted
	s.mu.Unlock()
	if muted {
		return nil
	}
	f := s.out.Pop()
	if f != nil {
		s.mu.Lock()
		s.outFrames++
		s.mu.Unlock()
	}
	return f
}

func (s *session) onPeerVideo(au []byte) {
	key := IsKeyframe(au)
	s.mu.Lock()
	s.videoIn++
	if key {
		s.videoInKey++
	}
	port, orientation := s.video, s.orientation
	s.mu.Unlock()
	if port != nil {
		port.deliver(VideoFrame{AccessUnit: au, Keyframe: key, Orientation: orientation})
	}
}

// peerVideoSink receives the peer's access units and the per-frame display rotation
// (RTP CVO). Phones render by the in-band rotation, not the <video> stanza, so it is
// what the client must use to show the picture upright.
type peerVideoSink struct{ s *session }

func (p peerVideoSink) WriteVideo(au []byte) error { p.s.onPeerVideo(au); return nil }
func (p peerVideoSink) Close() error               { return nil }
func (p peerVideoSink) SetOrientation(o int)       { p.s.onPeerOrientation(o) }

func (s *session) onPeerOrientation(orientation int) {
	s.mu.Lock()
	changed := s.orientation != orientation
	s.orientation = orientation
	active := s.info.PeerVideoActive
	port := s.video
	s.mu.Unlock()
	if !changed {
		return
	}
	if port != nil {
		port.control(VideoControl{Type: "peer_video", Active: active, Orientation: orientation})
	}
	s.m.emit(&Video{CallID: s.id, PeerVideoActive: active, Orientation: orientation})
}

// Peer <video> states that say whether its camera is on (others are upgrade signalling).
const (
	peerVideoDisabled = 0
	peerVideoEnabled  = 1
	peerVideoStopped  = 6
)

func (s *session) onVideoState(v meowcaller.VideoState) {
	s.mu.Lock()
	switch v.Raw {
	case peerVideoEnabled:
		s.info.PeerVideoActive = true
	case peerVideoDisabled, peerVideoStopped:
		s.info.PeerVideoActive = false
	}
	s.orientation = v.Orientation
	// Our camera already on means the agent already chose video: accept the peer's
	// request at once instead of leaving its phone waiting.
	autoAccept := v.Upgrade && s.info.VideoEnabled && !s.ended
	if v.Upgrade && !autoAccept {
		s.upgradeAsk = true
	}
	active := s.info.PeerVideoActive
	port := s.video
	ev := &Video{CallID: s.id, PeerVideoActive: active, Orientation: v.Orientation, UpgradeRequested: v.Upgrade && !autoAccept}
	s.mu.Unlock()
	if autoAccept {
		if err := s.call.AcceptVideo(); err != nil {
			s.m.log.Warn().Err(err).Str("call_id", s.id).Msg("auto-accepting peer video upgrade failed")
		} else {
			s.m.log.Info().Str("call_id", s.id).Msg("peer video upgrade auto-accepted (our camera is on)")
		}
	}
	s.m.log.Info().Str("call_id", s.id).Int("state", v.Raw).Bool("upgrade", v.Upgrade).Bool("auto_accept", autoAccept).
		Int("orientation", v.Orientation).Msg("peer video state")
	if port != nil {
		port.control(VideoControl{Type: "peer_video", Active: active, Orientation: v.Orientation})
	}
	s.m.emit(ev)
}

func (s *session) onKeyframeRequest() {
	s.mu.Lock()
	port := s.video
	s.mu.Unlock()
	if port != nil {
		port.control(VideoControl{Type: "keyframe_request"})
	}
}

func (s *session) sendVideo(au []byte, duration time.Duration) error {
	s.mu.Lock()
	enabled := s.info.VideoEnabled
	s.mu.Unlock()
	if !enabled {
		return nil
	}
	err := s.call.SendVideoWithDuration(au, duration)
	s.mu.Lock()
	if err != nil {
		s.videoErrs++
	} else {
		s.videoOut++
	}
	s.mu.Unlock()
	return err
}

func (s *session) stats() AudioStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return AudioStats{InFrames: s.inFrames, OutFrames: s.outFrames, InPeak: s.inPeak}
}

// sessionSource adapts the session's agent audio queue to meowcaller.AudioSource.
type sessionSource struct{ s *session }

func (src sessionSource) ReadFrame() ([]float32, error) { return src.s.nextOutFrame(), nil }
func (src sessionSource) Close() error                  { return nil }
