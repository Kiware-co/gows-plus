package server

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/devlikeapro/gows/calls"
	"github.com/devlikeapro/gows/gows"
	__ "github.com/devlikeapro/gows/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const callStatsEvery = 5 * time.Second

func (s *Server) callsFor(session *__.Session) (*calls.Manager, error) {
	cli, err := s.Sm.Get(session.GetId())
	if err != nil {
		return nil, err
	}
	if cli.Calls == nil {
		if !gows.CallsEnabled() {
			return nil, status.Error(codes.Unavailable, "calls engine is off (WAHA_GOWS_CALLS_ENABLED)")
		}
		return nil, status.Error(codes.Unavailable, "calls engine not installed for this session; restart it")
	}
	return cli.Calls, nil
}

// callError maps calls errors to gRPC codes (WAHA maps those to HTTP).
func callError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, calls.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, calls.ErrDisabled), errors.Is(err, calls.ErrNotAllowed):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, calls.ErrAlreadyAnswered), errors.Is(err, calls.ErrWrongState),
		errors.Is(err, calls.ErrBusy), errors.Is(err, calls.ErrAttached):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, calls.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, calls.ErrInvalidPhone):
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func toCallInfo(i calls.Info) *__.CallInfo {
	return &__.CallInfo{
		CallId:          i.CallID,
		Direction:       i.Direction,
		Phone:           i.Phone,
		ChatId:          i.ChatID,
		IsVideo:         i.IsVideo,
		State:           i.State,
		Muted:           i.Muted,
		VideoEnabled:    i.VideoEnabled,
		PeerVideoActive: i.PeerVideoActive,
		AnsweredBy:      i.AnsweredBy,
		StartedAt:       i.StartedAt,
		ConnectedAt:     i.ConnectedAt,
	}
}

func toCallSettings(st calls.Settings) *__.CallSettings {
	return &__.CallSettings{Enabled: st.Enabled, Allow: st.Allow, Available: st.Available}
}

func (s *Server) GetCallSettings(ctx context.Context, req *__.Session) (*__.CallSettings, error) {
	m, err := s.callsFor(req)
	if err != nil {
		return nil, err
	}
	return toCallSettings(m.Settings()), nil
}

func (s *Server) SetCallSettings(ctx context.Context, req *__.SetCallSettingsRequest) (*__.CallSettings, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	st, err := m.SetSettings(req.GetEnabled(), req.GetAllow())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return toCallSettings(st), nil
}

func (s *Server) ListCalls(ctx context.Context, req *__.Session) (*__.CallInfoList, error) {
	m, err := s.callsFor(req)
	if err != nil {
		return nil, err
	}
	out := &__.CallInfoList{}
	for _, i := range m.List() {
		out.Calls = append(out.Calls, toCallInfo(i))
	}
	return out, nil
}

func (s *Server) GetCall(ctx context.Context, req *__.CallRequest) (*__.CallInfo, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	i, err := m.Get(req.GetCallId())
	if err != nil {
		return nil, callError(err)
	}
	return toCallInfo(i), nil
}

func (s *Server) StartCall(ctx context.Context, req *__.StartCallRequest) (*__.CallInfo, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	i, err := m.Start(ctx, req.GetPhone(), req.GetVideo())
	if err != nil {
		return nil, callError(err)
	}
	return toCallInfo(i), nil
}

func (s *Server) AnswerCall(ctx context.Context, req *__.AnswerCallRequest) (*__.CallInfo, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	i, err := m.Answer(req.GetCallId(), req.GetAgent())
	if errors.Is(err, calls.ErrAlreadyAnswered) {
		return nil, status.Errorf(codes.FailedPrecondition, "call already answered by %q", i.AnsweredBy)
	}
	if err != nil {
		return nil, callError(err)
	}
	return toCallInfo(i), nil
}

func (s *Server) RejectManagedCall(ctx context.Context, req *__.CallRequest) (*__.Empty, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	return &__.Empty{}, callError(m.Reject(req.GetCallId()))
}

func (s *Server) HangupCall(ctx context.Context, req *__.CallRequest) (*__.Empty, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	return &__.Empty{}, callError(m.Hangup(req.GetCallId()))
}

func (s *Server) MuteCall(ctx context.Context, req *__.MuteCallRequest) (*__.CallInfo, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	i, err := m.Mute(req.GetCallId(), req.GetMuted())
	if err != nil {
		return nil, callError(err)
	}
	return toCallInfo(i), nil
}

func (s *Server) SetCallVideo(ctx context.Context, req *__.SetCallVideoRequest) (*__.CallInfo, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	i, err := m.SetVideo(req.GetCallId(), req.GetEnabled())
	if err != nil {
		return nil, callError(err)
	}
	return toCallInfo(i), nil
}

func (s *Server) SetCallOrientation(ctx context.Context, req *__.SetCallOrientationRequest) (*__.Empty, error) {
	m, err := s.callsFor(req.GetSession())
	if err != nil {
		return nil, err
	}
	return &__.Empty{}, callError(m.SetOrientation(req.GetCallId(), int(req.GetOrientation())))
}

// CallAudio bridges one client to a call's audio until the call ends or either side
// closes.
func (s *Server) CallAudio(stream grpc.BidiStreamingServer[__.CallAudioIn, __.CallAudioOut]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	attach := first.GetAttach()
	if attach == nil {
		return status.Error(codes.InvalidArgument, "first message must carry attach")
	}
	m, err := s.callsFor(attach.GetSession())
	if err != nil {
		return err
	}
	port, err := m.AttachAudio(attach.GetCallId())
	if err != nil {
		return callError(err)
	}
	defer port.Detach()

	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if msg.GetFlush() {
				port.Flush()
			}
			if pcm := msg.GetPcm(); len(pcm) > 0 {
				port.Write(pcm)
			}
		}
	}()

	ticker := time.NewTicker(callStatsEvery)
	defer ticker.Stop()
	for {
		select {
		case <-port.Done():
			return nil
		case <-stream.Context().Done():
			return nil
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case pcm := <-port.Frames():
			if err := stream.Send(&__.CallAudioOut{Pcm: pcm}); err != nil {
				return err
			}
		case <-ticker.C:
			st := port.Stats()
			if err := stream.Send(&__.CallAudioOut{Stats: &__.CallAudioStats{
				InFrames: int64(st.InFrames), OutFrames: int64(st.OutFrames), InPeak: float64(st.InPeak),
			}}); err != nil {
				return err
			}
		}
	}
}

// CallVideo bridges one client to a call's video.
func (s *Server) CallVideo(stream grpc.BidiStreamingServer[__.CallVideoIn, __.CallVideoOut]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	attach := first.GetAttach()
	if attach == nil {
		return status.Error(codes.InvalidArgument, "first message must carry attach")
	}
	m, err := s.callsFor(attach.GetSession())
	if err != nil {
		return err
	}
	port, err := m.AttachVideo(attach.GetCallId())
	if err != nil {
		return callError(err)
	}
	defer port.Detach()

	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if msg.GetSetOrientation() {
				_ = port.SetOrientation(int(msg.GetOrientation()))
			}
			if au := msg.GetAccessUnit(); len(au) > 0 {
				_ = port.Send(au, time.Duration(msg.GetDurationMs())*time.Millisecond)
			}
		}
	}()

	for {
		select {
		case <-port.Done():
			return nil
		case <-stream.Context().Done():
			return nil
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case f := <-port.Frames():
			if err := stream.Send(&__.CallVideoOut{AccessUnit: f.AccessUnit, Keyframe: f.Keyframe, Orientation: int32(f.Orientation)}); err != nil {
				return err
			}
		case c := <-port.Controls():
			if err := stream.Send(&__.CallVideoOut{Control: c.Type, Active: c.Active, Orientation: int32(c.Orientation)}); err != nil {
				return err
			}
		}
	}
}
