package graph

import (
	"log/slog"
	"nf-shard-worker/graph/model"
	"nf-shard-worker/pkg/cache"
	"nf-shard-worker/pkg/runner"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

type Resolver struct {
	NatsConn     *nats.Conn
	Logger       *slog.Logger
	NFService    runner.Runner
	FloatService runner.Runner
	Wg           *sync.WaitGroup
	Nc           *nats.Conn
	Js           jetstream.JetStream
	LogCache     *cache.Cache[model.Log]
}
