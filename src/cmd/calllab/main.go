// Command calllab is a local test bench for WhatsApp voice calls on a linked device.
//
// It links a device by QR (written as a PNG), then either listens for calls from
// allowlisted numbers (answers, records the caller to WAV and optionally echoes their
// voice back) or places one call to an allowlisted number and plays an audio file.
// Calls from any other number are never touched: the phone keeps ringing normally.
//
// It is a prototype for the gows engine integration, not part of the server binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/devlikeapro/gows/calls"
	_ "github.com/mattn/go-sqlite3"
	"github.com/purpshell/meowcaller"
	"github.com/purpshell/meowcaller/diag"
	"github.com/rs/zerolog"
	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type options struct {
	mode        string
	dataDir     string
	qrPath      string
	allow       string
	target      string
	play        string
	echo        bool
	answerAfter time.Duration
	maxDuration time.Duration
	diagDir     string
	verbose     bool
}

func main() {
	var o options
	flag.StringVar(&o.mode, "mode", "listen", "pair | listen | call")
	flag.StringVar(&o.dataDir, "data", "./calllab-data", "directory for the device store and recordings")
	flag.StringVar(&o.qrPath, "qr", "", "where to write the pairing QR as PNG (default <data>/qr.png)")
	flag.StringVar(&o.allow, "allow", "", "comma-separated phone numbers allowed to call / be called (required)")
	flag.StringVar(&o.target, "target", "", "number to call in -mode call (must be in -allow)")
	flag.StringVar(&o.play, "play", "", "audio file (.wav/.mp3/.opus) to play into the call")
	flag.BoolVar(&o.echo, "echo", true, "listen mode: play the caller's own voice back after ~1 s")
	flag.DurationVar(&o.answerAfter, "answer-after", 4*time.Second, "listen mode: ring this long before answering (a human is slower than a bot)")
	flag.DurationVar(&o.maxDuration, "max", 90*time.Second, "hang up after this long")
	flag.StringVar(&o.diagDir, "diag", "", "write meowcaller diagnostics (contains key material, local only) to this dir")
	flag.BoolVar(&o.verbose, "v", false, "debug logging")
	flag.Parse()

	level := zerolog.InfoLevel
	if o.verbose {
		level = zerolog.DebugLevel
	}
	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05.000"}).
		Level(level).With().Timestamp().Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = log.WithContext(ctx)

	if err := run(ctx, o); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal().Err(err).Msg("calllab failed")
	}
}

func run(ctx context.Context, o options) error {
	log := zerolog.Ctx(ctx)
	allow, err := calls.ParseAllowlist(o.allow)
	if err != nil {
		return err
	}
	if allow.Len() == 0 && o.mode != "pair" {
		return errors.New("-allow is required: calllab never answers or calls an unlisted number")
	}
	if err := os.MkdirAll(o.dataDir, 0o700); err != nil {
		return err
	}
	if o.qrPath == "" {
		o.qrPath = filepath.Join(o.dataDir, "qr.png")
	}

	var rec *diag.Recorder
	if o.diagDir != "" {
		if rec, err = diag.NewRecorder(o.diagDir); err != nil {
			return fmt.Errorf("open diagnostics: %w", err)
		}
		defer rec.Close()
	}

	wa, err := openDevice(ctx, o)
	if err != nil {
		return err
	}
	resolve := func(ctx context.Context, lid types.JID) (types.JID, error) {
		if wa.Store.LIDs == nil {
			return types.EmptyJID, errors.New("lid store not ready")
		}
		return wa.Store.LIDs.GetPNForLID(ctx, lid)
	}
	gate := calls.NewOfferGate(allow, resolve, func(d calls.GateDecision) {
		ev := log.Info()
		if !d.Allowed {
			ev = log.Warn()
		}
		ev.Str("call_id", d.Offer.CallID).Str("phone", d.Phone).Bool("video", d.Offer.Video).
			Bool("engaged", d.Allowed).Str("reason", d.Reason).Msg("incoming offer")
	})
	// The call hook must be installed before whatsmeow connects.
	mc := meowcaller.NewClient(wa,
		meowcaller.WithLogger(log.With().Str("component", "meowcaller").Logger()),
		meowcaller.WithDiagnostics(rec),
		meowcaller.WithOfferGate(gate),
	)
	if err := mc.HookError(); err != nil {
		return fmt.Errorf("call hook unavailable for this whatsmeow build: %w", err)
	}

	if err := connect(ctx, wa, o.qrPath); err != nil {
		return err
	}
	defer wa.Disconnect()
	log.Info().Str("self", wa.Store.GetJID().String()).Strs("allow", allow.Numbers()).Msg("linked and connected")

	switch o.mode {
	case "pair":
		log.Info().Msg("paired; nothing else to do")
		return nil
	case "listen":
		return listen(ctx, mc, o)
	case "call":
		return place(ctx, mc, allow, o)
	default:
		return fmt.Errorf("unknown -mode %q", o.mode)
	}
}

func openDevice(ctx context.Context, o options) (*whatsmeow.Client, error) {
	store.DeviceProps.Os = proto.String("Foxxy Llamadas (prueba)")
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()
	// No history: this device only needs call signaling.
	store.DeviceProps.RequireFullSync = proto.Bool(false)
	store.DeviceProps.HistorySyncConfig.FullSyncDaysLimit = proto.Uint32(1)
	store.DeviceProps.HistorySyncConfig.FullSyncSizeMbLimit = proto.Uint32(1)

	waLevel := "WARN"
	if zerolog.Ctx(ctx).GetLevel() <= zerolog.DebugLevel {
		waLevel = "INFO"
	}
	dbPath := filepath.Join(o.dataDir, "device.db")
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+dbPath+"?_foreign_keys=on&_busy_timeout=5000", waLog.Stdout("db", waLevel, true))
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return nil, fmt.Errorf("load device: %w", err)
	}
	wa := whatsmeow.NewClient(device, waLog.Stdout("wa", waLevel, true))
	wa.EnableAutoReconnect = true
	return wa, nil
}

func connect(ctx context.Context, wa *whatsmeow.Client, qrPath string) error {
	log := zerolog.Ctx(ctx)
	if wa.Store.ID == nil {
		qr, err := wa.GetQRChannel(ctx)
		if err != nil {
			return err
		}
		if err := wa.Connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		for evt := range qr {
			switch evt.Event {
			case "code":
				if err := qrcode.WriteFile(evt.Code, qrcode.Medium, 512, qrPath); err != nil {
					return fmt.Errorf("write QR: %w", err)
				}
				log.Info().Str("png", qrPath).Dur("valid", evt.Timeout).Msg("scan the QR: WhatsApp > Linked devices")
			case "success":
				log.Info().Msg("QR scanned")
				_ = os.Remove(qrPath)
			default:
				log.Warn().Str("event", evt.Event).Msg("pairing event")
				_ = os.Remove(qrPath)
			}
		}
		if wa.Store.ID == nil {
			wa.Disconnect()
			return errors.New("pairing ended without a scan; run again for a fresh QR")
		}
	} else if err := wa.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := waitReady(ctx, wa, 90*time.Second); err != nil {
		return err
	}
	// Call signaling is only delivered to a device that announced itself available.
	if wa.Store.PushName == "" {
		wa.Store.PushName = "Foxxy"
	}
	if err := wa.SendPresence(ctx, types.PresenceAvailable); err != nil {
		log.Warn().Err(err).Msg("send presence failed; continuing")
	}
	return nil
}

// waitReady tolerates the planned disconnect (515) right after pairing.
func waitReady(ctx context.Context, wa *whatsmeow.Client, timeout time.Duration) error {
	ready := make(chan struct{}, 4)
	id := wa.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.Connected); ok {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
	})
	defer wa.RemoveEventHandler(id)
	deadline := time.After(timeout)
	for !(wa.IsConnected() && wa.IsLoggedIn()) {
		select {
		case <-ready:
		case <-deadline:
			return errors.New("timed out waiting for the WhatsApp connection")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func listen(ctx context.Context, mc *meowcaller.Client, o options) error {
	log := zerolog.Ctx(ctx)
	mc.OnIncomingCall(func(call *meowcaller.Call) {
		go handleIncoming(ctx, call, o)
	})
	log.Info().Dur("answer_after", o.answerAfter).Bool("echo", o.echo).Msg("waiting for calls from the allowlist; Ctrl+C to stop")
	<-ctx.Done()
	return ctx.Err()
}

func handleIncoming(ctx context.Context, call *meowcaller.Call, o options) {
	log := zerolog.Ctx(ctx).With().Str("call_id", call.ID()).Logger()
	log.Info().Str("peer", call.Peer().String()).Bool("video", call.IsVideo()).Msg("ringing")

	select {
	case <-time.After(o.answerAfter):
	case <-ctx.Done():
		return
	}
	if call.State() == meowcaller.CallPhaseEnded {
		log.Info().Msg("caller hung up before we answered")
		return
	}

	session := newRecording(log, o.dataDir, "in", call.ID())
	sinks := []meowcaller.AudioSink{session.wav}
	var echo *calls.Echo
	if o.echo {
		echo = calls.NewEcho(16, 200) // ~1 s delay, ~12 s buffer
		sinks = append(sinks, echo)
	}
	tee := calls.NewTee(sinks...)
	call.Receive(tee)
	wireLifecycle(ctx, log, call, tee, o.maxDuration)

	if err := call.Answer(); err != nil {
		log.Error().Err(err).Msg("answer failed")
		return
	}
	log.Info().Msg("answered")
	switch {
	case echo != nil:
		call.Play(echo)
	case o.play != "":
		if src, err := openSource(o.play); err == nil {
			call.Play(src)
		} else {
			log.Error().Err(err).Msg("open audio file")
		}
	}
}

func place(ctx context.Context, mc *meowcaller.Client, allow calls.Allowlist, o options) error {
	log := zerolog.Ctx(ctx)
	target, err := calls.OutboundTarget(allow, o.target)
	if err != nil {
		return err
	}
	var src meowcaller.AudioSource
	if o.play != "" {
		if src, err = openSource(o.play); err != nil {
			return fmt.Errorf("open %s: %w", o.play, err)
		}
	}
	call, err := mc.Call(ctx, target)
	if err != nil {
		return fmt.Errorf("place call: %w", err)
	}
	clog := log.With().Str("call_id", call.ID()).Logger()
	clog.Info().Str("target", target).Msg("calling")

	session := newRecording(clog, o.dataDir, "out", call.ID())
	tee := calls.NewTee(session.wav)
	call.Receive(tee)
	ended := wireLifecycle(ctx, clog, call, tee, o.maxDuration)
	call.OnReady(func() {
		clog.Info().Msg("media ready")
		if src == nil {
			return
		}
		p := call.Play(src)
		p.OnFinish(func() {
			clog.Info().Msg("audio file finished; hanging up in 3 s")
			time.AfterFunc(3*time.Second, func() { _ = call.Hangup() })
		})
	})

	select {
	case <-ended:
		return nil
	case <-ctx.Done():
		_ = call.Hangup()
		<-time.After(time.Second)
		return ctx.Err()
	}
}

type recording struct {
	wav meowcaller.AudioSink
}

func newRecording(log zerolog.Logger, dir, direction, callID string) recording {
	name := fmt.Sprintf("%s-%s-%s.wav", time.Now().Format("20060102-150405"), direction, sanitize(callID))
	path := filepath.Join(dir, name)
	wav, err := meowcaller.WAVRecorder(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("open recorder")
		return recording{}
	}
	log.Info().Str("path", path).Msg("recording peer audio")
	return recording{wav: wav}
}

// wireLifecycle logs state changes, reports inbound audio once per 5 s (frames and
// peak level, to tell real voice from silence), enforces the max duration and closes
// the sinks when the call ends. The returned channel closes on end.
func wireLifecycle(ctx context.Context, log zerolog.Logger, call *meowcaller.Call, tee *calls.Tee, maxDuration time.Duration) <-chan struct{} {
	ended := make(chan struct{})
	var once sync.Once
	start := time.Now()
	call.OnStateChange(func(p meowcaller.CallPhase) { log.Info().Int("phase", int(p)).Msg("call state") })
	call.OnPeerAccept(func() { log.Info().Msg("peer accepted") })
	call.OnMuteState(func(m bool) { log.Info().Bool("peer_muted", m).Msg("mute state") })
	call.OnEnd(func(reason string) {
		once.Do(func() {
			frames, peak := tee.Stats()
			log.Info().Str("reason", reason).Dur("duration", time.Since(start)).
				Int("inbound_frames", frames).Float32("inbound_peak", peak).Msg("call ended")
			_ = tee.Close()
			close(ended)
		})
	})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		limit := time.After(maxDuration)
		for {
			select {
			case <-ticker.C:
				frames, peak := tee.Stats()
				log.Info().Int("inbound_frames", frames).Float32("inbound_peak", peak).Msg("inbound audio")
			case <-limit:
				log.Info().Msg("max duration reached; hanging up")
				_ = call.Hangup()
				return
			case <-ended:
				return
			case <-ctx.Done():
				_ = call.Hangup()
				return
			}
		}
	}()
	return ended
}

func openSource(file string) (meowcaller.AudioSource, error) {
	switch strings.ToLower(filepath.Ext(file)) {
	case ".wav":
		return meowcaller.WAVFile(file)
	case ".mp3":
		return meowcaller.MP3File(file)
	case ".opus", ".ogg":
		return meowcaller.OpusFile(file)
	}
	return nil, fmt.Errorf("unsupported audio file %q (use .wav, .mp3 or .opus)", file)
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, s)
}
