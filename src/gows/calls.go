package gows

import (
	"os"

	"github.com/rs/zerolog"
)

// callsEnabled installs the WhatsApp call engine on every session. Off by default:
// without it the engine is never hooked and nothing changes. Each session still has
// its own switch (calls.Manager.SetSettings), also off by default.
var callsEnabled bool

// SetCallsEnabled turns the call engine on for sessions built afterwards.
func SetCallsEnabled(enabled bool) { callsEnabled = enabled }

// CallsEnabled reports whether the call engine is installed on new sessions.
func CallsEnabled() bool { return callsEnabled }

func callsLogger(session string) zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true, TimeFormat: "15:04:05"}).
		Level(zerolog.InfoLevel).With().Timestamp().Str("session", session).Logger()
}
