package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devlikeapro/gows/calls"
	"github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
)

const videoFPS = 15

// loadVideoLoop reads the pre-encoded test pattern (Annex-B H.264).
func loadVideoLoop(path string) (*calls.VideoLoop, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read video: %w", err)
	}
	return calls.NewVideoLoop(calls.SplitAccessUnits(b), videoFPS)
}

// wireVideo records the peer's video to a .h264 file and sends ours: the test pattern
// loop, or (reply "echo") the peer's own frames back. It logs counters every 5 s and
// stops when the call ends.
func wireVideo(ctx context.Context, log zerolog.Logger, call *meowcaller.Call, o options, direction string) {
	name := fmt.Sprintf("%s-%s-%s.h264", time.Now().Format("20060102-150405"), direction, sanitize(call.ID()))
	path := filepath.Join(o.dataDir, name)
	rec, err := meowcaller.AnnexBRecorder(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("open video recorder")
		rec = nil
	} else {
		log.Info().Str("path", path).Msg("recording peer video")
	}

	vctx, cancel := context.WithCancel(ctx)
	var counter calls.VideoCounter
	var echoSent, sendErrs atomic.Int64
	var firstErr sync.Once
	reportErr := func(err error) {
		sendErrs.Add(1)
		firstErr.Do(func() { log.Warn().Err(err).Msg("video frame not sent (first error)") })
	}
	echo := o.videoReply == "echo" && direction == "in"

	call.ReceiveVideo(meowcaller.VideoSinkFunc(func(au []byte) {
		counter.Observe(au)
		if rec != nil {
			_ = rec.WriteVideo(au)
		}
		if echo {
			if err := call.SendVideoWithDuration(au, time.Second/videoFPS); err != nil {
				reportErr(err)
			} else {
				echoSent.Add(1)
			}
		}
	}))
	call.OnVideoState(func(s meowcaller.VideoState) {
		log.Info().Bool("active", s.Active).Bool("upgrade", s.Upgrade).Int("orientation", s.Orientation).Int("raw", s.Raw).Msg("peer video state")
	})

	var loop *calls.VideoLoop
	if !echo {
		if loop, err = loadVideoLoop(o.videoFile); err != nil {
			log.Error().Err(err).Msg("load test pattern; sending no video")
		} else {
			call.OnVideoKeyframeRequest(func() {
				log.Debug().Msg("peer asked for a keyframe")
				loop.RequestKeyframe()
			})
			go loop.Run(vctx, call.SendVideoWithDuration, reportErr)
		}
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		report := func(msg string) {
			units, keys, bytes := counter.Stats()
			ev := log.Info().Int("video_in_frames", units).Int("video_in_keyframes", keys).Int("video_in_bytes", bytes).
				Int64("video_send_errors", sendErrs.Load())
			if loop != nil {
				ev = ev.Int("video_out_frames", loop.Sent())
			}
			if echo {
				ev = ev.Int64("video_echo_frames", echoSent.Load())
			}
			ev.Msg(msg)
		}
		for {
			select {
			case <-ticker.C:
				if call.State() == meowcaller.CallPhaseEnded {
					report("video totals")
					cancel()
					if rec != nil {
						_ = rec.Close()
					}
					return
				}
				report("video")
			case <-vctx.Done():
				report("video totals")
				if rec != nil {
					_ = rec.Close()
				}
				return
			}
		}
	}()
}
