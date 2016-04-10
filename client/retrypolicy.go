package client

import (
	"time"

	"github.com/chronos-tachyon/go-raft/rpc"
	pb "github.com/chronos-tachyon/go-raft/proto"
)

type RetryPolicy interface {
	GetTimeout(retrynum uint) time.Duration
	ShouldRetry(retrynum uint, err error) bool
}

type DefaultRetryPolicy struct{}

func (_ DefaultRetryPolicy) GetTimeout(retrynum uint) time.Duration {
	return (1 << retrynum) * time.Second
}

func (_ DefaultRetryPolicy) ShouldRetry(retrynum uint, err error) bool {
	rpcerr, ok := err.(rpc.Error)
	return ok && rpcerr.Status == pb.ClientResponse_TIMED_OUT && retrynum < 3
}
