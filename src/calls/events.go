package calls

// Call states, as exposed to WAHA.
const (
	StateRinging    = "ringing"    // incoming, waiting for Answer
	StateCalling    = "calling"    // outgoing, waiting for the peer
	StateConnecting = "connecting" // answered, media coming up
	StateActive     = "active"     // audio both ways
	StateEnded      = "ended"
)

// Directions.
const (
	DirectionIncoming = "incoming"
	DirectionOutgoing = "outgoing"
)

// End reasons reported in Ended.Reason.
const (
	ReasonHangupLocal       = "hangup_local"
	ReasonHangupRemote      = "hangup_remote"
	ReasonRejectedLocal     = "rejected_local"
	ReasonRejectedRemote    = "rejected_remote"
	ReasonAnsweredElsewhere = "answered_elsewhere"
	ReasonRejectedElsewhere = "rejected_elsewhere"
	ReasonMissed            = "missed"
	ReasonBusy              = "busy"
	ReasonFailed            = "failed"
)

// Info is the public state of one managed call.
type Info struct {
	CallID          string `json:"callId"`
	Direction       string `json:"direction"`
	Phone           string `json:"phone"`
	ChatID          string `json:"chatId"`
	IsVideo         bool   `json:"isVideo"`
	State           string `json:"state"`
	Muted           bool   `json:"muted"`
	VideoEnabled    bool   `json:"videoEnabled"`
	PeerVideoActive bool   `json:"peerVideoActive"`
	AnsweredBy      string `json:"answeredBy,omitempty"`
	StartedAt       int64  `json:"startedAt"`
	ConnectedAt     int64  `json:"connectedAt,omitempty"`
}

// Settings is a session's call switch.
type Settings struct {
	Enabled   bool     `json:"enabled"`
	Allow     []string `json:"allow"`
	Available bool     `json:"available"`
}

// The types below are emitted on the session event stream; WAHA receives them as
// "calls.Ringing", "calls.Connected", ... and maps them to webhook events.

// Ringing: an incoming call the engine took is ringing, or an outgoing offer went out.
type Ringing struct {
	Info
}

// Connected: audio flows both ways.
type Connected struct {
	Info
}

// Ended: the call is over.
type Ended struct {
	Info
	Reason      string `json:"reason"`
	Detail      string `json:"detail,omitempty"`
	DurationMs  int64  `json:"durationMs"`
	ConnectedMs int64  `json:"connectedMs"`
}

// AudioSilent: a connected call got no audio from the peer for SilentMs.
type AudioSilent struct {
	CallID   string `json:"callId"`
	SilentMs int64  `json:"silentMs"`
}

// AudioRestored: peer audio is back after AudioSilent.
type AudioRestored struct {
	CallID   string `json:"callId"`
	SilentMs int64  `json:"silentMs"`
}

// Video: the peer's camera state, orientation or an upgrade request changed.
type Video struct {
	CallID           string `json:"callId"`
	PeerVideoActive  bool   `json:"peerVideoActive"`
	Orientation      int    `json:"orientation"`
	UpgradeRequested bool   `json:"upgradeRequested"`
}
