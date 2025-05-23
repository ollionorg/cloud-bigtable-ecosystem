// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxycore

import (
	"context"
	"sync"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
)

// PreparedEntry is an entry in the prepared cache.
type PreparedEntry struct {
	PreparedFrame *frame.RawFrame
}

// PreparedCache a thread-safe cache for storing prepared queries.
type PreparedCache interface {
	// Store add an entry to the cache.
	Store(id string, entry *PreparedEntry)
	// Load retrieves an entry from the cache. `ok` is true if the entry is present; otherwise it's false.
	Load(id string) (entry *PreparedEntry, ok bool)
}

type SessionConfig struct {
	Keyspace string
	Version  primitive.ProtocolVersion
	Auth     Authenticator
	// PreparedCache a global cache share across sessions for storing previously prepared queries
	PreparedCache     PreparedCache
	ConnectTimeout    time.Duration
	HeartBeatInterval time.Duration
	IdleTimeout       time.Duration
	Logger            *zap.Logger
}

type Session struct {
	ctx       context.Context
	config    SessionConfig
	logger    *zap.Logger
	pools     sync.Map
	connected chan struct{}
	failed    chan error
}

func ConnectSession(ctx context.Context, cluster *Cluster, config SessionConfig) (*Session, error) {
	session := &Session{
		ctx:       ctx,
		config:    config,
		logger:    GetOrCreateNopLogger(config.Logger),
		pools:     sync.Map{},
		connected: make(chan struct{}),
		failed:    make(chan error, 1),
	}
	return session, nil
}
