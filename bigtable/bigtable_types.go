/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package bigtableclient

import (
	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	rh "github.com/ollionorg/cassandra-to-bigtable-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/tableConfig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MutationColumnData struct {
	ColumnFamily string
	Name         string
	Contents     []byte
}

type MutationData struct {
	MutationType   string
	MutationRowKey string
	MutationColumn []MutationColumnData
	ColumnFamily   string
}

type BulkOperationResponse struct {
	FailedRows string
}

type BigtableClient struct {
	Client          map[string]*bigtable.Client
	Logger          *zap.Logger
	SqlClient       btpb.BigtableClient
	BigTableConfig  BigTableConfig
	ResponseHandler rh.ResponseHandlerIface
	TableConfig     *tableConfig.TableConfig
	grpcConn        *grpc.ClientConn
}

type BigTableConfig struct {
	DatabaseName    string
	ConfigTableName string
	NumOfChannels   int
	InstanceName    string
	GCPProjectID    string
	MaxSessions     uint64
	MinSessions     uint64
	StaleReads      int
	KeyspaceFlatter bool
	ColumnFamily    string
	ProfileId       string
}
type ConnConfig struct {
	InstanceIDs   string
	NumOfChannels int
	GCPProjectID  string
	AppID         string
	Meterics      bigtable.MetricsProvider
}
