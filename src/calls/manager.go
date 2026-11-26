package calls

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// Errors returned by Manager; the gRPC layer maps them to status codes.
var (
	ErrUnavailable     = errors.New("calls: not available for this session")
	ErrDisabled        = errors.New("calls: disabled for this session")
	ErrNotFound        = errors.New("calls: call not found")
	ErrBusy            = errors.New("calls: another call is in progress")
	ErrAlreadyAnswered = errors.New("calls: call already answered")
	ErrWrongState      = errors.New("calls: call is not in a state that allows this")
	ErrAttached        = errors.New("calls: a client is already attached")
)

const (
	// silenceAfter is how long a connected call may go without peer audio before
	// AudioSilent is emitted.
	silenceAfter = 5 * time.Second
	// outQueueFrames bounds the agent audio queued towards WhatsApp (~1 s).
	outQueueFrames = 16
)

// liveCall is the part of *meowcaller.Call the manager drives; tests fake it.
type liveCall interface {
	ID() string
	Peer() types.JID
	IsVideo() bool
	State() meowcaller.CallPhase
	Answer() error
	Reject() error
	Hangup() error
	Play(src meowcaller.AudioSource) *meowcaller.Player
	Receive(sink meowcaller.AudioSink)
	ReceiveVideo(sink meowcaller.VideoSink)
	SendVideoWithDuration(accessUnit []byte, duration time.Duration) error
	SetVideoEnabled(enabled bool) error
	StartVideo() error
	AcceptVideo() error
	SetVideoOrientation(orientation int) error
	OnReady(fn func())
	OnEnd(fn func(reason string))
	OnStateChange(fn func(meowcaller.CallPhase))
	OnVideoState(fn func(meowcaller.VideoState))
	OnVideoKeyframeRequest(fn func())
}

// dialer places outgoing calls; *meowcaller.Client implements it.
type dialer interface {
	CallWithOptions(ctx context.Context, target string, opts meowcaller.CallOptions) (*meowcaller.Call, error)
}

// Manager runs WhatsApp calls for one session. Calls are off until SetSettings
// enables them; while off (or when the hook could not be installed) every offer is
// left to whatsmeow exactly as without this package.
type Manager struct {
	log     zerolog.Logger
	emit    func(any)
	resolve PNResolver
	ownUser func() []string
	dial    dialer
	hookErr error
	now     func() time.Time

	mu       sync.Mutex
	enabled  bool
	allow    Allowlist
	active   *session
	closed   bool
	stopTick chan struct{}
}

// New installs call handling on wa. It must run before wa connects. emit receives the
// event structs of this package.
func New(wa *whatsmeow.Client, log zerolog.Logger, emit func(any)) *Manager {
	m := newManager(log, emit)
	m.resolve = func(ctx context.Context, lid types.JID) (types.JID, error) {
		if wa.Store == nil || wa.Store.LIDs == nil {
			return types.EmptyJID, errors.New("lid store not ready")
		}
		return wa.Store.LIDs.GetPNForLID(ctx, lid)
	}
	m.ownUser = func() []string {
		var users []string
		if wa.Store == nil {
			return users
		}
		if jid := wa.Store.GetJID(); !jid.IsEmpty() {
			users = append(users, jid.User)
		}
		if lid := wa.Store.GetLID(); !lid.IsEmpty() {
			users = append(users, lid.User)
		}
		return users
	}
	mc := meowcaller.NewClient(wa,
		meowcaller.WithLogger(log.With().Str("component", "meowcaller").Logger()),
		meowcaller.WithOfferGate(m.gate),
	)
	m.dial = mc
	if err := mc.HookError(); err != nil {
		m.hookErr = err
		log.Error().Err(err).Msg("call hook unavailable; calls stay off for this session")
		return m
	}
	mc.OnIncomingCall(func(c *meowcaller.Call) { m.track(c, DirectionIncoming) })
	wa.AddEventHandler(m.onWhatsmeowEvent)
	go m.watchSilence()
	return m
}

func newManager(log zerolog.Logger, emit func(any)) *Manager {
	if emit == nil {
		emit = func(any) {}
	}
	return &Manager{
		log:      log,
		emit:     emit,
		now:      time.Now,
		ownUser:  func() []string { return nil },
		stopTick: make(chan struct{}),
	}
}

// Available reports why calls cannot work in this session, or nil.
func (m *Manager) Available() error {
	if m.hookErr != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, m.hookErr)
	}
	return nil
}

// Settings returns the session's call switch.
func (m *Manager) Settings() Settings {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Settings{Enabled: m.enabled, Allow: m.allow.Numbers(), Available: m.hookErr == nil}
}

// SetSettings turns calls on or off and replaces the allowlist ("*" = everyone).
// Turning calls off leaves a call in progress alone.
func (m *Manager) SetSettings(enabled bool, allow []string) (Settings, error) {
	list, err := NewAllowlist(allow)
	if err != nil {
		return Settings{}, err
	}
	m.mu.Lock()
	m.enabled = enabled
	m.allow = list
	m.mu.Unlock()
	m.log.Info().Bool("enabled", enabled).Int("allow", list.Len()).Bool("allow_all", list.All()).Msg("call settings updated")
	return m.Settings(), nil
}

// gate decides, per inbound offer, whether this device engages at all.
func (m *Manager) gate(offer meowcaller.Offer) bool {
	m.mu.Lock()
	enabled, allow, closed := m.enabled, m.allow, m.closed
	busy := m.active != nil
	m.mu.Unlock()
	if closed || !enabled || m.hookErr != nil {
		return false
	}
	d := decide(allow, m.resolve, offer)
	if d.Allowed && busy {
		d.Allowed, d.Reason = false, "busy"
	}
	// Every decision is logged; the caller's number only when engaged, so declined
	// callers (anyone who calls the number) stay out of the logs.
	m.log.Info().Str("call_id", offer.CallID).Bool("video", offer.Video).Bool("engaged", d.Allowed).
		Str("reason", d.Reason).Str("from_server", offer.From.Server).Bool("phone_resolved", d.Phone != "").
		Msg("incoming offer")
	return d.Allowed
}

// Start places an outgoing call to an allowlisted number.
func (m *Manager) Start(ctx context.Context, phone string, video bool) (Info, error) {
	if err := m.Available(); err != nil {
		return Info{}, err
	}
	m.mu.Lock()
	enabled, allow, busy := m.enabled, m.allow, m.active != nil
	m.mu.Unlock()
	if !enabled {
		return Info{}, ErrDisabled
	}
	target, err := OutboundTarget(allow, phone)
	if err != nil {
		return Info{}, err
	}
	if busy {
		return Info{}, ErrBusy
	}
	c, err := m.dial.CallWithOptions(ctx, target, meowcaller.CallOptions{Video: video})
	if err != nil {
		return Info{}, fmt.Errorf("calls: place call: %w", err)
	}
	s := m.track(c, DirectionOutgoing)
	if s == nil {
		_ = c.Hangup()
		return Info{}, ErrBusy
	}
	s.setPhone(strings.TrimPrefix(target, "+"))
	return s.snapshot(), nil
}

// List returns the calls in progress (at most one today).
func (m *Manager) List() []Info {
	m.mu.Lock()
	s := m.active
	m.mu.Unlock()
	if s == nil {
		return []Info{}
	}
	return []Info{s.snapshot()}
}

// Get returns one call in progress.
func (m *Manager) Get(callID string) (Info, error) {
	s, err := m.lookup(callID)
	if err != nil {
		return Info{}, err
	}
	return s.snapshot(), nil
}

// Answer picks up a ringing incoming call. The first caller wins; later ones get
// ErrAlreadyAnswered together with the winner's info.
func (m *Manager) Answer(callID, agent string) (Info, error) {
	s, err := m.lookup(callID)
	if err != nil {
		return Info{}, err
	}
	return s.answer(agent)
}

// Reject declines a ringing incoming call.
func (m *Manager) Reject(callID string) error {
	s, err := m.lookup(callID)
	if err != nil {
		return err
	}
	return s.reject()
}

// Hangup ends a call in any state.
func (m *Manager) Hangup(callID string) error {
	s, err := m.lookup(callID)
	if err != nil {
		return err
	}
	return s.hangup()
}

// Mute replaces the agent's audio with silence (or restores it).
func (m *Manager) Mute(callID string, muted bool) (Info, error) {
	s, err := m.lookup(callID)
	if err != nil {
		return Info{}, err
	}
	return s.setMuted(muted), nil
}

// SetVideo turns our camera on or off; on an audio call it asks the peer to upgrade,
// or accepts the peer's pending upgrade.
func (m *Manager) SetVideo(callID string, enabled bool) (Info, error) {
	s, err := m.lookup(callID)
	if err != nil {
		return Info{}, err
	}
	return s.setVideo(enabled)
}

// SetOrientation announces our camera rotation in quarter turns.
func (m *Manager) SetOrientation(callID string, orientation int) error {
	s, err := m.lookup(callID)
	if err != nil {
		return err
	}
	return s.call.SetVideoOrientation(orientation)
}

// AttachAudio hands the call's audio to one client.
func (m *Manager) AttachAudio(callID string) (*AudioPort, error) {
	s, err := m.lookup(callID)
	if err != nil {
		return nil, err
	}
	return s.attachAudio()
}

// AttachVideo hands the call's video to one client.
func (m *Manager) AttachVideo(callID string) (*VideoPort, error) {
	s, err := m.lookup(callID)
	if err != nil {
		return nil, err
	}
	return s.attachVideo()
}

// Close hangs up a call in progress and stops handling new ones.
func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	s := m.active
	m.mu.Unlock()
	close(m.stopTick)
	if s != nil {
		_ = s.hangup()
	}
}

func (m *Manager) lookup(callID string) (*session, error) {
	if err := m.Available(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.id != callID {
		return nil, ErrNotFound
	}
	return m.active, nil
}

// track starts managing c; nil when another call already holds the slot.
func (m *Manager) track(c liveCall, direction string) *session {
	now := m.now()
	s := &session{
		m:         m,
		call:      c,
		id:        c.ID(),
		direction: direction,
		started:   now,
		out:       NewFramer(outQueueFrames),
	}
	s.info = Info{
		CallID:       s.id,
		Direction:    direction,
		IsVideo:      c.IsVideo(),
		VideoEnabled: c.IsVideo(),
		StartedAt:    now.Unix(),
	}
	if direction == DirectionIncoming {
		s.info.State = StateRinging
	} else {
		s.info.State = StateCalling
	}
	if phone := phoneOf(c.Peer(), m.resolve); phone != "" {
		s.setPhone(phone)
	}

	m.mu.Lock()
	if m.active != nil {
		m.mu.Unlock()
		m.log.Warn().Str("call_id", s.id).Str("active", m.active.id).Msg("second call while busy; not handled")
		return nil
	}
	m.active = s
	m.mu.Unlock()

	s.wire()
	m.log.Info().Str("call_id", s.id).Str("direction", direction).Bool("video", s.info.IsVideo).Msg("call tracked")
	m.emit(&Ringing{Info: s.snapshot()})
	return s
}

func (m *Manager) release(s *session) {
	m.mu.Lock()
	if m.active == s {
		m.active = nil
	}
	m.mu.Unlock()
}

// onWhatsmeowEvent notices an incoming call answered on another device of the account
// (the model on her phone).
func (m *Manager) onWhatsmeowEvent(evt any) {
	accept, ok := evt.(*events.CallAccept)
	if !ok {
		return
	}
	m.mu.Lock()
	s := m.active
	m.mu.Unlock()
	if s == nil || s.id != accept.CallID || s.direction != DirectionIncoming {
		return
	}
	for _, user := range m.ownUser() {
		if accept.From.User == user {
			s.answeredElsewhere()
			return
		}
	}
}

func (m *Manager) watchSilence() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopTick:
			return
		case <-ticker.C:
			m.mu.Lock()
			s := m.active
			m.mu.Unlock()
			if s != nil {
				s.checkSilence(m.now())
			}
		}
	}
}

// endReason maps meowcaller's end reason plus what we know locally to Ended.Reason.
func endReason(raw string, f endFacts) (reason, detail string) {
	lower := strings.ToLower(raw)
	switch {
	case f.elsewhere:
		return ReasonAnsweredElsewhere, ""
	case f.localReject:
		return ReasonRejectedLocal, ""
	case f.localHangup:
		return ReasonHangupLocal, ""
	case lower == "accepted_elsewhere":
		return ReasonAnsweredElsewhere, ""
	case lower == "rejected_elsewhere":
		return ReasonRejectedElsewhere, ""
	case strings.HasPrefix(lower, "server:"):
		return ReasonFailed, raw
	case strings.Contains(lower, "busy"):
		return ReasonBusy, ""
	case lower == "rejected":
		return ReasonRejectedRemote, ""
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "missed"):
		return ReasonMissed, ""
	case f.connected:
		return ReasonHangupRemote, ""
	case strings.Contains(lower, "fail"), strings.Contains(lower, "error"):
		return ReasonFailed, raw
	default:
		return ReasonMissed, ""
	}
}

type endFacts struct {
	localHangup bool
	localReject bool
	elsewhere   bool
	connected   bool
}
