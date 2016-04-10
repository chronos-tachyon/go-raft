package rpc

import (
	"fmt"

	pb "github.com/chronos-tachyon/go-raft/proto"
)

type Error struct {
	Status       pb.ClientResponse_Status
	ErrorMessage string
	Redirect     string
}

func (err Error) Error() string {
	var suffix string
	if err.Redirect != "" {
		suffix = fmt.Sprintf(" (redirect to %q)", err.Redirect)
	}
	return fmt.Sprintf("%v: %s%s", err.Status, err.ErrorMessage, suffix)
}

func (err Error) AsClientResponse() *pb.ClientResponse {
	return &pb.ClientResponse{
		Status:       err.Status,
		ErrorMessage: err.ErrorMessage,
		Redirect:     err.Redirect,
	}
}

func TimedOutError() Error {
	return Error{
		Status:       pb.ClientResponse_TIMED_OUT,
		ErrorMessage: "server did not respond within deadline",
	}
}

func NotLeaderError(selfId, leaderId uint32, redirect string) Error {
	err := Error{
		Status:       pb.ClientResponse_NOT_LEADER,
		ErrorMessage: fmt.Sprintf("this is <%d>; the leader is <%d>", selfId, leaderId),
		Redirect:     redirect,
	}
	if leaderId == 0 {
		err.ErrorMessage = fmt.Sprintf("this is <%d>; don't know who the leader is", selfId)
	}
	return err
}

func InvalidSessionError(sid uint32) Error {
	return Error{
		Status:       pb.ClientResponse_INVALID_SESSION,
		ErrorMessage: fmt.Sprintf("unknown session %#08x", sid),
	}
}

func ErrorFromClientResponse(resp *pb.ClientResponse) error {
	if resp.Status == pb.ClientResponse_OK {
		return nil
	}
	return Error{
		Status:       resp.Status,
		ErrorMessage: resp.ErrorMessage,
		Redirect:     resp.Redirect,
	}
}

func ClientResponseFromError(err error) *pb.ClientResponse {
	if err == nil {
		return &pb.ClientResponse{Status: pb.ClientResponse_OK}
	}
	if rpcerr, ok := err.(Error); ok {
		return rpcerr.AsClientResponse()
	}
	return &pb.ClientResponse{
		Status:       pb.ClientResponse_UNKNOWN,
		ErrorMessage: err.Error(),
	}
}
