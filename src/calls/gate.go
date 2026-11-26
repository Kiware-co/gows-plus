package calls

import (
	"context"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types"
)

// PNResolver maps a hidden user (LID) JID to its phone-number JID.
// whatsmeow's store implements it as Store.LIDs.GetPNForLID.
type PNResolver func(ctx context.Context, lid types.JID) (types.JID, error)

// resolveTimeout bounds the store lookup: the gate runs on whatsmeow's event dispatch.
const resolveTimeout = 2 * time.Second

// GateDecision explains why an offer was engaged or ignored. Phone is empty when the
// caller's number could not be resolved.
type GateDecision struct {
	Offer   meowcaller.Offer
	Phone   string
	Allowed bool
	Reason  string
}

// NewOfferGate returns a meowcaller offer gate that engages only 1:1 offers whose
// caller resolves to an allowlisted phone number. onDecision (optional) observes
// every decision, for logging.
func NewOfferGate(allow Allowlist, resolve PNResolver, onDecision func(GateDecision)) func(meowcaller.Offer) bool {
	return func(offer meowcaller.Offer) bool {
		d := decide(allow, resolve, offer)
		if onDecision != nil {
			onDecision(d)
		}
		return d.Allowed
	}
}

func decide(allow Allowlist, resolve PNResolver, offer meowcaller.Offer) GateDecision {
	d := GateDecision{Offer: offer}
	if offer.Group {
		d.Reason = "group call"
		return d
	}
	if allow.Len() == 0 {
		d.Reason = "empty allowlist"
		return d
	}
	for _, jid := range []types.JID{offer.Creator, offer.From} {
		phone := phoneOf(jid, resolve)
		if phone == "" {
			continue
		}
		d.Phone = phone
		if allow.Allows(phone) {
			d.Allowed = true
			d.Reason = "allowlisted"
			return d
		}
	}
	if d.Phone == "" {
		d.Reason = "caller phone unknown"
	} else {
		d.Reason = "not allowlisted"
	}
	return d
}

func phoneOf(jid types.JID, resolve PNResolver) string {
	switch jid.Server {
	case types.DefaultUserServer, types.LegacyUserServer:
		return jid.User
	case types.HiddenUserServer:
		if resolve == nil {
			return ""
		}
		ctx, cancel := context.WithTimeout(context.Background(), resolveTimeout)
		defer cancel()
		pn, err := resolve(ctx, jid.ToNonAD())
		if err != nil || pn.IsEmpty() {
			return ""
		}
		return pn.User
	}
	return ""
}
