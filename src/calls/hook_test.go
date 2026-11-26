package calls

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

// TestHookInstallsOnPinnedWhatsmeow fails when a whatsmeow update moves the internals
// the call hook patches, so the breakage shows up in CI instead of as silent calls.
func TestHookInstallsOnPinnedWhatsmeow(t *testing.T) {
	ctx := context.Background()
	container, err := sqlstore.New(ctx, "sqlite3", "file:hooktest?mode=memory&cache=shared&_foreign_keys=on", nil)
	require.NoError(t, err)
	defer container.Close()
	wa := whatsmeow.NewClient(container.NewDevice(), nil)

	m := New(wa, zerolog.Nop(), nil)
	defer m.Close()
	require.NoError(t, m.Available())
	st := m.Settings()
	require.True(t, st.Available)
	require.False(t, st.Enabled, "calls start off")
}
