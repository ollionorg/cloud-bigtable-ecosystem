// Copyright (c) DataStax, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	bt "github.com/ollionorg/cassandra-to-bigtable-proxy/bigtable"
	otelgo "github.com/ollionorg/cassandra-to-bigtable-proxy/otel"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/proxycore"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/tableConfig"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/translator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// const (
// 	testAddr      = "127.0.0.1"
// 	testStartAddr = "127.0.0.0"
// )

// func generateTestPort() int {
// 	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
// 	if err != nil {
// 		log.Panicf("failed to resolve for local port: %v", err)
// 	}

// 	l, err := net.ListenTCP("tcp", addr)
// 	if err != nil {
// 		log.Panicf("failed to listen for local port: %v", err)
// 	}
// 	defer l.Close()
// 	return l.Addr().(*net.TCPAddr).Port
// }

// func generateTestAddr(baseAddress string, n int) string {
// 	ip := make(net.IP, net.IPv6len)
// 	new(big.Int).Add(new(big.Int).SetBytes(net.ParseIP(baseAddress)), big.NewInt(int64(n))).FillBytes(ip)
// 	return ip.String()
// }

// func generateTestAddrs(host string) (clusterPort int, clusterAddr, proxyAddr, httpAddr string) {
// 	clusterPort = generateTestPort()
// 	clusterAddr = net.JoinHostPort(host, strconv.Itoa(clusterPort))
// 	proxyPort := generateTestPort()
// 	proxyAddr = net.JoinHostPort(host, strconv.Itoa(proxyPort))
// 	httpPort := generateTestPort()
// 	httpAddr = net.JoinHostPort(host, strconv.Itoa(httpPort))
// 	return clusterPort, clusterAddr, proxyAddr, httpAddr
// }

func TestOnEvent(t *testing.T) {
	// logger := proxycore.GetOrCreateNopLogger(config.Logger)

	// p := Proxy{logger: logger}
	// p.OnEvent()
}

// func TestNewProxy(t *testing.T) {
// 	ctx := context.Background()
// 	var logger *zap.Logger
// 	logger = proxycore.GetOrCreateNopLogger(logger)

// 	relativePath := "fakedata/service_account.json"

// 	// Get the absolute path by resolving the relative path
// 	absolutePath, err := filepath.Abs(relativePath)
// 	if err != nil {
// 		assert.NoErrorf(t, err, "should not through an error")
// 	}
// 	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
// 	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)
// 	prox, err := NewProxy(ctx, Config{
// 		Version: primitive.ProtocolVersion4,
// 		Logger:  logger,
// 		SpannerConfig: SpannerConfig{
// 			DatabaseName: "projects/cassandra-to-spanner/instances/test-instance-v1/databases/test-database",
// 		},
// 	})
// 	assert.NotNilf(t, prox, "should not be nil")
// 	assert.NoErrorf(t, err, "should not through an error")
// }

// func TestGetBigtableConnections(t *testing.T) {
// 	ctx := context.Background()
// 	os.Setenv("BIGTABLE_EMULATOR_HOST", "localhost:8086")
// 	var logger *zap.Logger
// 	logger = proxycore.GetOrCreateNopLogger(logger)
// 	_, err := getBigtableConnection(ctx, Config{
// 		Logger: logger,
// 	})
// 	assert.Errorf(t, err, "function should return error")

// 	res, err := getBigtableConnection(ctx, Config{
// 		BigTableConfig: BigTableConfig{
// 			DatabaseName: "key_space",
// 		},
// 	})
// 	assert.NoError(t, err, "function should return no error")
// 	assert.NotNilf(t, res, "function should return not nil")
// }

func TestConnect(t *testing.T) {
	var logger *zap.Logger
	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	prox := &Proxy{
		clients: mp,
		logger:  logger,
	}
	err := prox.Connect()
	assert.NoError(t, err, "function should return no error")
}

func Test_GetCqlQueryType(t *testing.T) {
	str := getCqlQueryType("select * from xyx")
	assert.NotEmptyf(t, str, "should not be empty")

	str = getCqlQueryType("update * from xyx")
	assert.NotEmptyf(t, str, "should not be empty")

	str = getCqlQueryType("insert * into xyx")
	assert.NotEmptyf(t, str, "should not be empty")

	str = getCqlQueryType("delete * from xyx")
	assert.NotEmptyf(t, str, "should not be empty")

	str = getCqlQueryType("negative scenario")
	assert.Emptyf(t, str, "should be empty")
}

func TestNameBasedUUID(t *testing.T) {
	uuid := nameBasedUUID("test")
	assert.NotNilf(t, uuid, "should not be nil")
}

func Test_addclient(t *testing.T) {
	relativePath := "fakedata/service_account.json"

	// Get the absolute path by resolving the relative path
	absolutePath, err := filepath.Abs(relativePath)
	if err != nil {
		assert.NoErrorf(t, err, "should not through an error")
	}
	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)

	mp := make(map[*client]struct{})
	prox := &Proxy{
		clients: mp,
	}

	assert.NoErrorf(t, err, "no error expected")
	cl := &client{
		ctx:                 prox.ctx,
		proxy:               prox,
		preparedSystemQuery: make(map[[preparedIdSize]byte]interface{}),
		preparedQuerys:      make(map[[preparedIdSize]byte]interface{}),
	}
	prox.addClient(cl)
	prox.registerForEvents(cl)
}

func Test_OnEvent(t *testing.T) {
	var logger *zap.Logger
	relativePath := "fakedata/service_account.json"

	// Get the absolute path by resolving the relative path
	absolutePath, err := filepath.Abs(relativePath)
	if err != nil {
		assert.NoErrorf(t, err, "should not through an error")
	}
	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)

	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	prox := &Proxy{
		clients: mp,
		logger:  logger,
	}
	assert.NoErrorf(t, err, "no error expected")
	prox.OnEvent(&proxycore.SchemaChangeEvent{Message: &message.SchemaChangeEvent{ChangeType: primitive.SchemaChangeType(primitive.EventTypeSchemaChange)}})

}

func Test_Serve(t *testing.T) {
	var logger *zap.Logger
	logger = proxycore.GetOrCreateNopLogger(logger)
	relativePath := "fakedata/service_account.json"

	// Get the absolute path by resolving the relative path
	absolutePath, err := filepath.Abs(relativePath)
	if err != nil {
		assert.NoErrorf(t, err, "should not through an error")
	}
	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)

	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	ls := make(map[*net.Listener]struct{})
	prox := &Proxy{
		clients:   mp,
		logger:    logger,
		listeners: ls,
	}
	assert.NoErrorf(t, err, "no error expected")

	var httpListener net.Listener
	httpListener, err = resolveAndListen(":7777", "", "")
	assert.NoErrorf(t, err, "no error expected")

	prox.isConnected = true
	go func() {
		time.Sleep(5 * time.Second)
		prox.isClosing = true
		httpListener.Close()
	}()
	err = prox.Serve(httpListener)
	assert.Errorf(t, err, "error expected")

}

func Test_ServeScenario2(t *testing.T) {
	var logger *zap.Logger
	relativePath := "fakedata/service_account.json"

	// Get the absolute path by resolving the relative path
	absolutePath, err := filepath.Abs(relativePath)
	if err != nil {
		assert.NoErrorf(t, err, "should not through an error")
	}
	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)

	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	ls := make(map[*net.Listener]struct{})
	cl := make(chan struct{})
	prox := &Proxy{
		clients:   mp,
		logger:    logger,
		listeners: ls,
		closed:    cl,
	}
	assert.NoErrorf(t, err, "no error expected")
	assert.NoErrorf(t, err, "no error expected")

	var httpListener net.Listener
	httpListener, err = resolveAndListen(":7777", "", "")
	assert.NoErrorf(t, err, "no error expected")

	prox.isConnected = true
	go func() {
		time.Sleep(5 * time.Second)
		close(prox.closed)
		httpListener.Close()
	}()
	err = prox.Serve(httpListener)
	assert.Errorf(t, err, "error expected")

}

// func TestProxy_ListenAndServe(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())

// 	tester, proxyContactPoint, err := setupProxyTest(ctx, 3, proxycore.MockRequestHandlers{
// 		primitive.OpCodeQuery: func(cl *proxycore.MockClient, frm *frame.Frame) message.Message {
// 			if msg := cl.InterceptQuery(frm.Header, frm.Body.Message.(*message.Query)); msg != nil {
// 				return msg
// 			} else {
// 				column, err := proxycore.EncodeType(datatype.Varchar, frm.Header.Version, net.JoinHostPort(cl.Local().IP, strconv.Itoa(cl.Local().Port)))
// 				if err != nil {
// 					return &message.ServerError{ErrorMessage: "Unable to encode type"}
// 				}
// 				return &message.RowsResult{
// 					Metadata: &message.RowsMetadata{
// 						Columns: []*message.ColumnMetadata{
// 							{
// 								Keyspace: "test",
// 								Table:    "test",
// 								Name:     "host",
// 								Type:     datatype.Varchar,
// 							},
// 						},
// 						ColumnCount: 1,
// 					},
// 					Data: message.RowSet{{
// 						column,
// 					}},
// 				}
// 			}
// 		},
// 	})
// 	defer func() {
// 		cancel()
// 		tester.shutdown()
// 	}()
// 	require.NoError(t, err)

// 	cl := connectTestClient(t, ctx, proxyContactPoint)

// 	hosts, err := queryTestHosts(ctx, cl)
// 	require.NoError(t, err)
// 	assert.Equal(t, 3, len(hosts))

// 	tester.cluster.Stop(1)

// 	removed := waitUntil(10*time.Second, func() bool {
// 		hosts, err := queryTestHosts(ctx, cl)
// 		require.NoError(t, err)
// 		return len(hosts) == 2
// 	})
// 	assert.True(t, removed)

// 	err = tester.cluster.Start(ctx, 1)
// 	require.NoError(t, err)

// 	added := waitUntil(10*time.Second, func() bool {
// 		hosts, err := queryTestHosts(ctx, cl)
// 		require.NoError(t, err)
// 		return len(hosts) == 3
// 	})
// 	assert.True(t, added)
// }

// func TestProxy_Unprepared(t *testing.T) {
// 	const numNodes = 3
// 	const version = primitive.ProtocolVersion4

// 	preparedId := []byte("abc")
// 	var prepared sync.Map

// 	ctx, cancel := context.WithCancel(context.Background())
// 	tester, proxyContactPoint, err := setupProxyTest(ctx, numNodes, proxycore.MockRequestHandlers{
// 		primitive.OpCodePrepare: func(cl *proxycore.MockClient, frm *frame.Frame) message.Message {
// 			prepared.Store(cl.Local().IP, true)
// 			return &message.PreparedResult{
// 				PreparedQueryId: preparedId,
// 			}
// 		},
// 		primitive.OpCodeExecute: func(cl *proxycore.MockClient, frm *frame.Frame) message.Message {
// 			if _, ok := prepared.Load(cl.Local().IP); ok {
// 				return &message.RowsResult{
// 					Metadata: &message.RowsMetadata{
// 						ColumnCount: 0,
// 					},
// 					Data: message.RowSet{},
// 				}
// 			} else {
// 				ex := frm.Body.Message.(*message.Execute)
// 				assert.Equal(t, preparedId, ex.QueryId)
// 				return &message.Unprepared{Id: ex.QueryId}
// 			}
// 		},
// 	})
// 	defer func() {
// 		cancel()
// 		tester.shutdown()
// 	}()
// 	require.NoError(t, err)

// 	cl := connectTestClient(t, ctx, proxyContactPoint)

// 	// Only prepare on a single node
// 	resp, err := cl.SendAndReceive(ctx, frame.NewFrame(version, 0, &message.Prepare{Query: "SELECT * FROM test.test"}))
// 	require.NoError(t, err)
// 	assert.Equal(t, primitive.OpCodeResult, resp.Header.OpCode)
// 	_, ok := resp.Body.Message.(*message.PreparedResult)
// 	assert.True(t, ok, "expected prepared result")

// 	for i := 0; i < numNodes; i++ {
// 		resp, err = cl.SendAndReceive(ctx, frame.NewFrame(version, 0, &message.Execute{QueryId: preparedId}))
// 		require.NoError(t, err)
// 		assert.Equal(t, primitive.OpCodeResult, resp.Header.OpCode)
// 		_, ok = resp.Body.Message.(*message.RowsResult)
// 		assert.True(t, ok, "expected rows result")
// 	}

// 	// Count the number of unique nodes that were prepared
// 	count := 0
// 	prepared.Range(func(_, _ interface{}) bool {
// 		count++
// 		return true
// 	})
// 	assert.Equal(t, numNodes, count)
// }

// func TestProxy_UseKeyspace(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	tester, proxyContactPoint, err := setupProxyTest(ctx, 1, nil)
// 	defer func() {
// 		cancel()
// 		tester.shutdown()
// 	}()
// 	require.NoError(t, err)

// 	cl := connectTestClient(t, ctx, proxyContactPoint)

// 	resp, err := cl.SendAndReceive(ctx, frame.NewFrame(primitive.ProtocolVersion4, 0, &message.Query{Query: "USE system"}))
// 	require.NoError(t, err)

// 	assert.Equal(t, primitive.OpCodeResult, resp.Header.OpCode)
// 	res, ok := resp.Body.Message.(*message.SetKeyspaceResult)
// 	require.True(t, ok, "expected set keyspace result")
// 	assert.Equal(t, "system", res.Keyspace)
// }

// func TestProxy_NegotiateProtocolV5(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	tester, proxyContactPoint, err := setupProxyTest(ctx, 1, nil)
// 	defer func() {
// 		cancel()
// 		tester.shutdown()
// 	}()
// 	require.NoError(t, err)

// 	cl, err := proxycore.ConnectClient(ctx, proxycore.NewEndpoint(proxyContactPoint), proxycore.ClientConnConfig{})
// 	require.NoError(t, err)

// 	version, err := cl.Handshake(ctx, primitive.ProtocolVersion5, nil)
// 	require.NoError(t, err)
// 	assert.Equal(t, primitive.ProtocolVersion4, version) // Expected to be negotiated to v4
// }

// func TestProxy_DseVersion(t *testing.T) {
// 	const dseVersion = "6.8.3"
// 	const protocol = primitive.ProtocolVersion4

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	// Fake a peer so that "SELECT ... system.peers" returns at least one row
// 	tester, proxyContactPoint, err := setupProxyTestWithConfig(ctx, 1, &proxyTestConfig{
// 		dseVersion: dseVersion,
// 		rpcAddr:    "127.0.0.1",
// 		peers: []PeerConfig{{
// 			RPCAddr: "127.0.0.2",
// 		}}})
// 	defer func() {
// 		cancel()
// 		tester.shutdown()
// 	}()
// 	require.NoError(t, err)

// 	cl := connectTestClient(t, ctx, proxyContactPoint)

// 	checkDseVersion := func(resp *frame.Frame, err error) {
// 		require.NoError(t, err)
// 		assert.Equal(t, primitive.OpCodeResult, resp.Header.OpCode)
// 		rows, ok := resp.Body.Message.(*message.RowsResult)
// 		assert.True(t, ok, "expected rows result")

// 		rs := proxycore.NewResultSet(rows, protocol)
// 		require.GreaterOrEqual(t, rs.RowCount(), 1)
// 		actualDseVersion, err := rs.Row(0).StringByName("dse_version")
// 		require.NoError(t, err)
// 		assert.Equal(t, dseVersion, actualDseVersion)
// 	}

// 	checkDseVersion(cl.SendAndReceive(ctx, frame.NewFrame(protocol, 0, &message.Query{Query: "SELECT dse_version FROM system.local"})))
// 	checkDseVersion(cl.SendAndReceive(ctx, frame.NewFrame(primitive.ProtocolVersion4, 0, &message.Query{Query: "SELECT dse_version FROM system.peers"})))
// }

// func queryTestHosts(ctx context.Context, cl *proxycore.ClientConn) (map[string]struct{}, error) {
// 	hosts := make(map[string]struct{})
// 	for i := 0; i < 3; i++ {
// 		rs, err := cl.Query(ctx, primitive.ProtocolVersion4, &message.Query{Query: "SELECT * FROM test.test"})
// 		if err != nil {
// 			return nil, err
// 		}
// 		if rs.RowCount() < 1 {
// 			return nil, errors.New("invalid row count")
// 		}
// 		val, err := rs.Row(0).ByName("host")
// 		if err != nil {
// 			return nil, err
// 		}
// 		hosts[val.(string)] = struct{}{}
// 	}
// 	return hosts, nil
// }

// type proxyTester struct {
// 	cluster *proxycore.MockCluster
// 	proxy   *Proxy
// 	wg      sync.WaitGroup
// }

// func (w *proxyTester) shutdown() {
// 	w.cluster.Shutdown()
// 	_ = w.proxy.Close()
// 	w.wg.Wait()
// }

// func setupProxyTest(ctx context.Context, numNodes int, handlers proxycore.MockRequestHandlers) (tester *proxyTester, proxyContactPoint string, err error) {
// 	return setupProxyTestWithConfig(ctx, numNodes, &proxyTestConfig{handlers: handlers})
// }

// type proxyTestConfig struct {
// 	handlers        proxycore.MockRequestHandlers
// 	dseVersion      string
// 	rpcAddr         string
// 	peers           []PeerConfig
// 	idempotentGraph bool
// }

// func setupProxyTestWithConfig(ctx context.Context, numNodes int, cfg *proxyTestConfig) (tester *proxyTester, proxyContactPoint string, err error) {
// 	tester = &proxyTester{
// 		wg: sync.WaitGroup{},
// 	}

// 	clusterPort, clusterAddr, proxyAddr, _ := generateTestAddrs(testAddr)

// 	tester.cluster = proxycore.NewMockCluster(net.ParseIP(testStartAddr), clusterPort)
// 	tester.cluster.DseVersion = cfg.dseVersion

// 	if cfg == nil {
// 		cfg = &proxyTestConfig{}
// 	}

// 	if cfg.handlers != nil {
// 		tester.cluster.Handlers = proxycore.NewMockRequestHandlers(cfg.handlers)
// 	}

// 	for i := 1; i <= numNodes; i++ {
// 		err = tester.cluster.Add(ctx, i)
// 		if err != nil {
// 			return tester, proxyAddr, err
// 		}
// 	}

// 	tester.proxy, _ = NewProxy(ctx, Config{
// 		Version:           primitive.ProtocolVersion4,
// 		Resolver:          proxycore.NewResolverWithDefaultPort([]string{clusterAddr}, clusterPort),
// 		ReconnectPolicy:   proxycore.NewReconnectPolicyWithDelays(200*time.Millisecond, time.Second),
// 		NumConns:          2,
// 		HeartBeatInterval: 30 * time.Second,
// 		ConnectTimeout:    10 * time.Second,
// 		IdleTimeout:       60 * time.Second,
// 		RPCAddr:           cfg.rpcAddr,
// 		Peers:             cfg.peers,
// 		IdempotentGraph:   cfg.idempotentGraph,
// 	})

// 	err = tester.proxy.Connect()
// 	if err != nil {
// 		return tester, proxyAddr, err
// 	}

// 	l, err := resolveAndListen(proxyAddr, "", "")
// 	if err != nil {
// 		return tester, proxyAddr, err
// 	}

// 	tester.wg.Add(1)

// 	go func() {
// 		_ = tester.proxy.Serve(l)
// 		tester.wg.Done()
// 	}()

// 	return tester, proxyAddr, nil
// }

// func connectTestClient(t *testing.T, ctx context.Context, proxyContactPoint string) *proxycore.ClientConn {
// 	cl, err := proxycore.ConnectClient(ctx, proxycore.NewEndpoint(proxyContactPoint), proxycore.ClientConnConfig{})
// 	require.NoError(t, err)

// 	version, err := cl.Handshake(ctx, primitive.ProtocolVersion4, nil)
// 	require.NoError(t, err)
// 	assert.Equal(t, primitive.ProtocolVersion4, version)

// 	return cl
// }

// func waitUntil(d time.Duration, check func() bool) bool {
// 	iterations := int(d / (100 * time.Millisecond))
// 	for i := 0; i < iterations; i++ {
// 		if check() {
// 			return true
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// 	return false
// }

// func Test_client_prepareDeleteType(t *testing.T) {

// 	mockRawFrame := &frame.RawFrame{
// 		Header: &frame.Header{
// 			Version:  primitive.ProtocolVersion4,
// 			Flags:    0,
// 			StreamId: 0,
// 			OpCode:   primitive.OpCodePrepare,
// 		},
// 		Body: []byte{},
// 	}
// 	relativePath := "fakedata/service_account.json"

// 	// Get the absolute path by resolving the relative path
// 	absolutePath, err := filepath.Abs(relativePath)
// 	if err != nil {
// 		assert.NoErrorf(t, err, "should not through an error")
// 	}
// 	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
// 	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)

// 	mockColMeta := []*message.ColumnMetadata{
// 		{Name: "test_id", Type: datatype.Varchar, Index: 0},
// 		{Name: "test_hash", Type: datatype.Varchar, Index: 1},
// 	}

// 	tableConfig := &tableConfig.TableConfig{
// 		TablesMetaData: []tableConfig.TableMetaData{
// 			{
// 				TableName:    "test_table",
// 				KeySpaceName: "key_space",
// 				Columns: []tableConfig.Column{
// 					{
// 						ColumnName: "test_id",
// 						ColumnType: "text",
// 					},
// 					{
// 						ColumnName: "test_hash",
// 						ColumnType: "text",
// 					},
// 				},
// 			},
// 		},
// 		ColumnMetadataCache: map[string]map[string]message.ColumnMetadata{
// 			"test_table": {
// 				"test_id": message.ColumnMetadata{
// 					Name: "test_id",
// 					Type: datatype.Varchar,
// 				},
// 				"test_hash": message.ColumnMetadata{
// 					Name: "test_hash",
// 					Type: datatype.Varchar,
// 				},
// 			},
// 		},

// 		Logger: zap.NewNop(),
// 	}

// 	mockProxy := &Proxy{
// 		tableConfig: tableConfig,
// 		translator:  &translator.Translator{TableConfig: tableConfig},
// 		logger:      zap.NewNop(),
// 	}

// 	type fields struct {
// 		ctx                 context.Context
// 		proxy               *Proxy
// 		conn                *proxycore.Conn
// 		keyspace            string
// 		preparedSystemQuery map[[16]byte]interface{}
// 		preparedQuerys      map[[16]byte]interface{}
// 	}

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		raw     *frame.RawFrame
// 		query   *message.Prepare
// 		id      [16]byte
// 		modify  func(*client)
// 		setup   func(*client)
// 		want    []*message.ColumnMetadata
// 		want1   []*message.ColumnMetadata
// 		wantErr bool
// 	}{

// 		{
// 			name: "Success Case",
// 			fields: fields{
// 				proxy: mockProxy,
// 			},
// 			setup: func(c *client) {
// 				c.preparedSystemQuery = make(map[[16]byte]interface{})
// 				c.preparedQuerys = make(map[[16]byte]interface{})
// 			},
// 			id:  [16]byte{10},
// 			raw: mockRawFrame,
// 			query: &message.Prepare{
// 				Query: "DELETE FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
// 			},
// 			want:    mockColMeta,
// 			want1:   mockColMeta,
// 			wantErr: false,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			c := &client{
// 				ctx:                 tt.fields.ctx,
// 				proxy:               mockProxy,
// 				conn:                tt.fields.conn,
// 				keyspace:            tt.fields.keyspace,
// 				preparedSystemQuery: tt.fields.preparedSystemQuery,
// 				preparedQuerys:      tt.fields.preparedQuerys,
// 			}
// 			if tt.setup != nil {
// 				tt.setup(c)
// 			}
// 			if tt.modify != nil {
// 				tt.modify(c)
// 			}
// 			got, _, err := c.prepareDeleteType(tt.raw, tt.query, tt.id)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("client.prepareDeleteType() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("client.prepareDeleteType() got = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// func Test_client_prepareInsertType(t *testing.T) {
// 	mockRawFrame := &frame.RawFrame{
// 		Header: &frame.Header{
// 			Version:  primitive.ProtocolVersion4,
// 			Flags:    0,
// 			StreamId: 0,
// 			OpCode:   primitive.OpCodePrepare,
// 		},
// 		Body: []byte{},
// 	}
// 	relativePath := "fakedata/service_account.json"

// 	// Get the absolute path by resolving the relative path
// 	absolutePath, err := filepath.Abs(relativePath)
// 	if err != nil {
// 		assert.NoErrorf(t, err, "should not through an error")
// 	}
// 	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
// 	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)

// 	mockColMeta := []*message.ColumnMetadata{
// 		{Name: "test_id", Type: datatype.Varchar, Index: 0},
// 		{Name: "test_hash", Type: datatype.Varchar, Index: 1},
// 	}

// 	tableConfig := &tableConfig.TableConfig{
// 		TablesMetaData:  mockTableConfig,
// 		PkMetadataCache: mockPkMetadata,

// 		Logger: zap.NewNop(),
// 	}

// 	mockProxy := &Proxy{
// 		tableConfig: tableConfig,
// 		translator:  &translator.Translator{TableConfig: tableConfig},
// 		logger:      zap.NewNop(),
// 	}

// 	type fields struct {
// 		ctx                 context.Context
// 		proxy               *Proxy
// 		conn                *proxycore.Conn
// 		keyspace            string
// 		preparedSystemQuery map[[16]byte]interface{}
// 		preparedQuerys      map[[16]byte]interface{}
// 	}

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		raw     *frame.RawFrame
// 		query   *message.Prepare
// 		id      [16]byte
// 		setup   func(*client)
// 		want    []*message.ColumnMetadata
// 		want1   []*message.ColumnMetadata
// 		wantErr bool
// 	}{
// 		{
// 			name: "Success Case",
// 			fields: fields{
// 				proxy: mockProxy,
// 			},
// 			setup: func(c *client) {
// 				c.preparedSystemQuery = make(map[[16]byte]interface{})
// 				c.preparedQuerys = make(map[[16]byte]interface{})
// 			},
// 			id:  [16]byte{10},
// 			raw: mockRawFrame,
// 			query: &message.Prepare{
// 				Query: "INSERT INTO key_space.test_table (test_id, test_hash) VALUES ('?', '?')",
// 			},
// 			want:    mockColMeta,
// 			want1:   mockColMeta,
// 			wantErr: false,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			c := &client{
// 				ctx:                 tt.fields.ctx,
// 				proxy:               mockProxy,
// 				conn:                tt.fields.conn,
// 				keyspace:            tt.fields.keyspace,
// 				preparedSystemQuery: tt.fields.preparedSystemQuery,
// 				preparedQuerys:      tt.fields.preparedQuerys,
// 			}
// 			if tt.setup != nil {
// 				tt.setup(c)
// 			}
// 			got, got1, err := c.prepareInsertType(tt.raw, tt.query, tt.id)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("client.prepareInsertType() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("client.prepareInsertType() got = %v, want %v", got, tt.want)
// 			}
// 			if !reflect.DeepEqual(got1, tt.want1) {
// 				t.Errorf("client.prepareInsertType() got1 = %v, want %v", got1, tt.want1)
// 			}
// 		})
// 	}
// }

// func Test_client_prepareSelectType(t *testing.T) {
// 	mockRawFrame := &frame.RawFrame{
// 		Header: &frame.Header{
// 			Version:  primitive.ProtocolVersion4,
// 			Flags:    0,
// 			StreamId: 0,
// 			OpCode:   primitive.OpCodePrepare,
// 		},
// 		Body: []byte{},
// 	}

// 	mockColMeta := []*message.ColumnMetadata{
// 		{Name: "test_id", Type: datatype.Varchar, Index: 0},
// 		{Name: "test_hash", Type: datatype.Varchar, Index: 1},
// 	}

// 	relativePath := "fakedata/service_account.json"

// 	// Get the absolute path by resolving the relative path
// 	absolutePath, err := filepath.Abs(relativePath)
// 	if err != nil {
// 		assert.NoErrorf(t, err, "should not through an error")
// 	}
// 	newPath := strings.Replace(absolutePath, "/proxy/", "/", 1)
// 	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", newPath)
// 	tableConfig := &tableConfig.TableConfig{
// 		TablesMetaData: []tableConfig.TableMetaData{
// 			{
// 				TableName:    "test_table",
// 				KeySpaceName: "key_space",
// 				Columns: []tableConfig.Column{
// 					{
// 						ColumnName: "test_id",
// 						ColumnType: "text",
// 					},
// 					{
// 						ColumnName: "test_hash",
// 						ColumnType: "text",
// 					},
// 				},
// 			},
// 		},
// 		ColumnMetadataCache: map[string]map[string]message.ColumnMetadata{
// 			"test_table": {
// 				"test_id": message.ColumnMetadata{
// 					Name: "test_id",
// 					Type: datatype.Varchar,
// 				},
// 				"test_hash": message.ColumnMetadata{
// 					Name: "test_hash",
// 					Type: datatype.Varchar,
// 				},
// 			},
// 		},
// 		Logger: zap.NewNop(),
// 	}

// 	mockProxy := &Proxy{
// 		tableConfig: tableConfig,
// 		translator:  &translator.Translator{TableConfig: tableConfig},
// 		logger:      zap.NewNop(),
// 	}

// 	type fields struct {
// 		ctx                 context.Context
// 		proxy               *Proxy
// 		conn                *proxycore.Conn
// 		keyspace            string
// 		preparedSystemQuery map[[16]byte]interface{}
// 		preparedQuerys      map[[16]byte]interface{}
// 	}

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		raw     *frame.RawFrame
// 		query   *message.Prepare
// 		id      [16]byte
// 		setup   func(*client)
// 		want    []*message.ColumnMetadata
// 		want1   []*message.ColumnMetadata
// 		wantErr bool
// 	}{
// 		{
// 			name: "Success Case",
// 			fields: fields{
// 				proxy: mockProxy,
// 			},
// 			setup: func(c *client) {
// 				c.preparedSystemQuery = make(map[[16]byte]interface{})
// 				c.preparedQuerys = make(map[[16]byte]interface{})
// 			},
// 			id:  [16]byte{10},
// 			raw: mockRawFrame,
// 			query: &message.Prepare{
// 				Query: "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
// 			},
// 			want:    mockColMeta,
// 			want1:   mockColMeta,
// 			wantErr: false,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			c := &client{
// 				ctx:                 tt.fields.ctx,
// 				proxy:               mockProxy,
// 				conn:                tt.fields.conn,
// 				keyspace:            tt.fields.keyspace,
// 				preparedSystemQuery: tt.fields.preparedSystemQuery,
// 				preparedQuerys:      tt.fields.preparedQuerys,
// 			}
// 			if tt.setup != nil {
// 				tt.setup(c)
// 			}
// 			got, got1, err := c.prepareSelectType(tt.raw, tt.query, tt.id)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("client.prepareSelectType() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("client.prepareSelectType() got = %v, want %v", got, tt.want)
// 			}
// 			if !reflect.DeepEqual(got1, tt.want1) {
// 				t.Errorf("client.prepareSelectType() got1 = %v, want %v", got1, tt.want1)
// 			}
// 		})
// 	}
// }

type MockQueryPreparer struct {
	DeleteCalled bool
	DeleteParams struct {
		Raw *frame.RawFrame
		Msg *message.Prepare
		Id  [16]byte
	}
	InsertCalled bool
	InsertParams struct {
		Raw *frame.RawFrame
		Msg *message.Prepare
		Id  [16]byte
	}
	SelectCalled bool
	SelectParams struct {
		Raw *frame.RawFrame
		Msg *message.Prepare
		Id  [16]byte
	}
}

type MockClient struct {
	SendFunc func(header *frame.RawFrame, preparedResult *message.PreparedResult) error
}

func (m *MockClient) Send(header *frame.RawFrame, preparedResult *message.PreparedResult) error {
	if m.SendFunc != nil {
		return m.SendFunc(header, preparedResult)
	}
	return nil
}

type mockSender struct {
	SendCalled bool
	SendParams struct {
		Hdr *frame.Header
		Msg message.Message
	}
}

func (m *mockSender) Send(hdr *frame.Header, msg message.Message) {
	m.SendCalled = true
	m.SendParams.Hdr = hdr
	m.SendParams.Msg = msg
}

var mockRawFrame = &frame.RawFrame{
	Header: &frame.Header{
		Version:  primitive.ProtocolVersion4,
		Flags:    0,
		StreamId: 0,
		OpCode:   primitive.OpCodePrepare,
	},
	Body: []byte{},
}
var tableConfigs = &tableConfig.TableConfig{
	TablesMetaData:  mockTableConfig,
	PkMetadataCache: mockPkMetadata,
	Logger:          zap.NewNop(),
}

var mockProxy = &Proxy{
	tableConfig: tableConfigs,
	translator:  &translator.Translator{TableConfig: tableConfigs},
	logger:      zap.NewNop(),
}

/*
func Test_client_handleServerPreparedQuery(t *testing.T) {
	mockProxy := &Proxy{
		tableConfig: tableConfigs,
		translator:  &translator.Translator{TableConfig: tableConfigs},
		logger:      zap.NewNop(),
		ctx:         context.Background(),
		//bClient:     mockBigtableClient,
		bClient: nil,
		otelInst: &otelgo.OpenTelemetry{Config: &otelgo.OTelConfig{
			ServiceName: "test",
			OTELEnabled: false,
		}},
	}
	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
		preparedQuerys      map[[16]byte]interface{}
	}

	tests := []struct {
		name      string
		fields    fields
		raw       *frame.RawFrame
		query     *message.Prepare
		queryType string
		id        [16]byte
		setup     func(*client)
		want      []*message.ColumnMetadata
		want1     []*message.ColumnMetadata
		wantErr   bool
	}{
		// {
		// 	name: "Select Query Test",
		// 	fields: fields{
		// 		proxy: mockProxy,
		// 	},
		// 	raw: mockRawFrame,
		// 	setup: func(c *client) {
		// 		c.preparedSystemQuery = make(map[[16]byte]interface{})
		// 		c.preparedQuerys = make(map[[16]byte]interface{})
		// 	},
		// 	query: &message.Prepare{Query: "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?'"},
		// },
		{
			name: "Insert Query Test",
			fields: fields{
				proxy: mockProxy,
			},
			queryType: "insert",
			raw:       mockRawFrame,
			query:     &message.Prepare{Query: "INSERT INTO key_space.test_table (test_id, test_hash) VALUES ('?', '?')"},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.preparedQuerys = make(map[[16]byte]interface{})
			},
		},
		{
			name: "Delete Query Test",
			fields: fields{
				proxy: mockProxy,
			},
			queryType: "delete",

			raw:   mockRawFrame,
			query: &message.Prepare{Query: "DELETE FROM key_space.test_table WHERE test_id = '?'"},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.preparedQuerys = make(map[[16]byte]interface{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryPreparer := &MockQueryPreparer{}
			mockSender := &mockSender{}
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               mockProxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				preparedQuerys:      tt.fields.preparedQuerys,
				sender:              mockSender,
			}
			if tt.setup != nil {
				tt.setup(c)
			}
			c.handleServerPreparedQuery(tt.raw, tt.query, tt.queryType)

			if mockQueryPreparer.DeleteCalled {
				if !reflect.DeepEqual(mockQueryPreparer.DeleteParams.Raw, tt.raw) {
					t.Errorf("PrepareDeleteType was not called with the correct RawFrame")
				}
				if !reflect.DeepEqual(mockQueryPreparer.DeleteParams.Msg, tt.query) {
					t.Errorf("PrepareDeleteType was not called with the correct Prepare message")
				}
			}

			if mockQueryPreparer.InsertCalled {
				if !reflect.DeepEqual(mockQueryPreparer.InsertParams.Raw, tt.raw) {
					t.Errorf("PrepareInsertType was not called with the correct RawFrame")
				}
				if !reflect.DeepEqual(mockQueryPreparer.InsertParams.Msg, tt.query) {
					t.Errorf("PrepareInsertType was not called with the correct Prepare message")
				}
			}

			if mockQueryPreparer.SelectCalled {
				if !reflect.DeepEqual(mockQueryPreparer.SelectParams.Raw, tt.raw) {
					t.Errorf("PrepareSelectType was not called with the correct RawFrame")
				}
				if !reflect.DeepEqual(mockQueryPreparer.SelectParams.Msg, tt.query) {
					t.Errorf("PrepareSelectType was not called with the correct Prepare message")
				}
			}
		})
	}
}
*/

// Create mock for handleExecutionForDeletePreparedQuery/handleExecutionForSelectPreparedQuery/handleExecutionForInsertPreparedQuery functions.
type MockBigtableClient struct {
	InsertRowFunc          func(ctx context.Context, data *translator.InsertQueryMap) error
	UpdateRowFunc          func(ctx context.Context, data *translator.UpdateQueryMap) error
	DeleteRowFunc          func(ctx context.Context, data *translator.DeleteQueryMap) error
	GetTableConfigsFunc    func(ctx context.Context, tableName string) (map[string]map[string]*tableConfig.Column, map[string][]tableConfig.Column, error)
	InsertErrorDetailsFunc func(ctx context.Context, query responsehandler.ErrorDetail)
	ApplyBulkMutationFunc  func(ctx context.Context, tableName string, mutationData []bt.MutationData) (bt.BulkOperationResponse, error)
}

func (m *MockBigtableClient) InsertRow(ctx context.Context, data *translator.InsertQueryMap) error {
	if m.InsertRowFunc != nil {
		return m.InsertRowFunc(ctx, data)
	}
	return nil
}

func (m *MockBigtableClient) UpdateRow(ctx context.Context, data *translator.UpdateQueryMap) error {
	if m.UpdateRowFunc != nil {
		return m.UpdateRowFunc(ctx, data)
	}
	return nil
}

func (m *MockBigtableClient) DeleteRow(ctx context.Context, data *translator.DeleteQueryMap) error {
	if m.DeleteRowFunc != nil {
		return m.DeleteRowFunc(ctx, data)
	}
	return nil
}
func (m *MockBigtableClient) GetTableConfigs(ctx context.Context, tableName string) (map[string]map[string]*tableConfig.Column, map[string][]tableConfig.Column, error) {
	if m.GetTableConfigsFunc != nil {
		return m.GetTableConfigsFunc(ctx, tableName)
	}
	return nil, nil, nil
}

func (m *MockBigtableClient) InsertErrorDetails(ctx context.Context, query responsehandler.ErrorDetail) {
	if m.InsertErrorDetailsFunc != nil {
		m.InsertErrorDetailsFunc(ctx, query)
	}
}
func (m *MockBigtableClient) ApplyBulkMutation(ctx context.Context, tableName string, mutationData []bt.MutationData) (bt.BulkOperationResponse, error) {
	if m.ApplyBulkMutationFunc != nil {
		return m.ApplyBulkMutationFunc(ctx, tableName, mutationData)
	}
	return bt.BulkOperationResponse{}, nil
}

var mockBigtableClient = &MockBigtableClient{
	InsertRowFunc: func(ctx context.Context, data *translator.InsertQueryMap) error {
		return nil
	},
	UpdateRowFunc: func(ctx context.Context, data *translator.UpdateQueryMap) error {
		return nil
	},
	DeleteRowFunc: func(ctx context.Context, data *translator.DeleteQueryMap) error {
		return nil
	},
	GetTableConfigsFunc: func(ctx context.Context, tableName string) (map[string]map[string]*tableConfig.Column, map[string][]tableConfig.Column, error) {
		return map[string]map[string]*tableConfig.Column{}, map[string][]tableConfig.Column{}, nil
	},
	ApplyBulkMutationFunc: func(ctx context.Context, tableName string, mutationData []bt.MutationData) (bt.BulkOperationResponse, error) {
		return bt.BulkOperationResponse{}, nil
	},
	InsertErrorDetailsFunc: func(ctx context.Context, query responsehandler.ErrorDetail) {
	},
}

var positionValues = []*primitive.Value{
	{Type: primitive.ValueTypeNull, Contents: []byte("value1")},
	{Type: primitive.ValueTypeNull, Contents: []byte("value2")},
}

var namedValues = map[string]*primitive.Value{
	"test_id":   {Type: primitive.ValueTypeNull, Contents: []byte("value1")},
	"test_hash": {Type: primitive.ValueTypeNull, Contents: []byte("value2")},
}

func Test_handleExecutionForDeletePreparedQuery(t *testing.T) {

	mockProxy := &Proxy{
		tableConfig: tableConfigs,
		translator:  &translator.Translator{TableConfig: tableConfigs},
		logger:      zap.NewNop(),
		ctx:         context.Background(),
		//bClient:     mockBigtableClient, // update client to include all function
		bClient: nil, // update client to include all function
		otelInst: &otelgo.OpenTelemetry{Config: &otelgo.OTelConfig{
			ServiceName: "test",
			OTELEnabled: false,
		}},
	}

	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
		preparedQuerys      map[[16]byte]interface{}
	}
	type args struct {
		raw           *frame.RawFrame
		msg           *partialExecute
		customPayload map[string][]byte
		st            *translator.DeleteQueryMap
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Test handleExecutionForDeletePreparedQuery Query",
			fields: fields{
				ctx:   context.Background(),
				proxy: mockProxy,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.DeleteQueryMap{
					Table:     "test_table",
					Query:     "DELETE FROM key_space.test_table WHERE test_id = '?'",
					ParamKeys: []string{"test_id", "test_hash"},
					// TranslatedQuery: "DELETE FROM key_space.test_table WHERE test_id = '?'",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSender := &mockSender{}
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               tt.fields.proxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				preparedQuerys:      tt.fields.preparedQuerys,
				sender:              mockSender,
			}
			c.handleExecuteForDelete(tt.args.raw, tt.args.msg, tt.args.st, tt.fields.ctx)
			assert.Nil(t, c.preparedSystemQuery)
			assert.Nil(t, c.preparedQuerys)
		})
	}
}

// func Test_client_handleExecutionForSelectPreparedQuery(t *testing.T) {
// 	mockProxy := &Proxy{
// 		tableConfig: tableConfigs,
// 		translator:  &translator.Translator{TableConfig: tableConfigs},
// 		logger:      zap.NewNop(),
// 		ctx:         context.Background(),
// 		bClient:     mockBigtableClient,
// 	}

// 	type fields struct {
// 		ctx                 context.Context
// 		proxy               *Proxy
// 		conn                *proxycore.Conn
// 		keyspace            string
// 		preparedSystemQuery map[[16]byte]interface{}
// 		preparedQuerys      map[[16]byte]interface{}
// 	}
// 	type args struct {
// 		raw           *frame.RawFrame
// 		msg           *partialExecute
// 		customPayload map[string][]byte
// 		st            *translator.SelectQueryMap
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 	}{
// 		{
// 			name: "Test handleExecutionForDeletePreparedQuery Query",
// 			fields: fields{
// 				ctx:   context.Background(),
// 				proxy: mockProxy,
// 			},

// 			args: args{
// 				raw: mockRawFrame,
// 				msg: &partialExecute{
// 					PositionalValues: positionValues,
// 					NamedValues:      namedValues,
// 				},
// 				customPayload: map[string][]byte{
// 					"test_id":   []byte("1234"),
// 					"test_hash": []byte("1234-2345"),
// 				},
// 				st: &translator.SelectQueryMap{
// 					Table:           "test_table",
// 					Query:           "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
// 					ParamKeys:       []string{"test_id", "test_hash"},
// 					TranslatedQuery: "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
// 				},
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			mockSender := &mockSender{}
// 			c := &client{
// 				ctx:                 tt.fields.ctx,
// 				proxy:               tt.fields.proxy,
// 				conn:                tt.fields.conn,
// 				keyspace:            tt.fields.keyspace,
// 				preparedSystemQuery: tt.fields.preparedSystemQuery,
// 				preparedQuerys:      tt.fields.preparedQuerys,
// 				sender:              mockSender,
// 			}
// 			c.handleExecutionForSelectPreparedQuery(tt.args.raw, tt.args.msg, tt.args.customPayload, tt.args.st)
// 			assert.Nil(t, c.preparedSystemQuery)
// 			assert.Nil(t, c.preparedQuerys)
// 		})
// 	}
// }

/*
func Test_client_handleExecutionForInsertPreparedQuery(t *testing.T) {

	mockProxy := &Proxy{
		tableConfig: tableConfigs,
		translator:  &translator.Translator{TableConfig: tableConfigs},
		logger:      zap.NewNop(),
		ctx:         context.Background(),
		//bClient:     mockBigtableClient,
		bClient: nil,
		otelInst: &otelgo.OpenTelemetry{Config: &otelgo.OTelConfig{
			ServiceName: "test",
			OTELEnabled: false,
		}},
	}

	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
		preparedQuerys      map[[16]byte]interface{}
	}
	type args struct {
		raw           *frame.RawFrame
		msg           *partialExecute
		customPayload map[string][]byte
		st            *translator.InsertQueryMap
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Test handleExecutionForDeletePreparedQuery Query",
			fields: fields{
				ctx:   context.Background(),
				proxy: mockProxy,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.InsertQueryMap{
					Table:           "test_table",
					Query:           "INSERT INTO key_space.test_table (test_id, test_hash) VALUES (?, ?)",
					ParamKeys:       []string{"test_id", "test_hash"},
					TranslatedQuery: "INSERT INTO key_space.test_table (test_id, test_hash) VALUES ('param1, 'param2')",
					Columns: []translator.Column{
						{
							Name:         "test_id",
							ColumnFamily: "family1",
							CQLType:      "text",
						},
						{
							Name:         "test_id",
							ColumnFamily: "family2",
							CQLType:      "text",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSender := &mockSender{}
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               tt.fields.proxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				preparedQuerys:      tt.fields.preparedQuerys,
				sender:              mockSender,
			}
			c.handleExecuteForInsert(tt.args.raw, tt.args.msg, tt.args.st, tt.fields.ctx)
			assert.Nil(t, c.preparedSystemQuery)
			assert.Nil(t, c.preparedQuerys)
		})
	}
}*/

type MockTranslator struct {
	mock.Mock
}

// func (m *MockTranslator) TranslateSelectQuerytoBigtable(query string) (responsehandler.QueryMetadata, error) {
// 	args := m.Called(query)
// 	return args.Get(0).(responsehandler.QueryMetadata), args.Error(1)
// }

func (m *MockTranslator) TranslateInsertQuerytoBigtable(queryStr string, protocolV primitive.ProtocolVersion) (*translator.InsertQueryMap, error) {
	args := m.Called(queryStr)
	return args.Get(0).(*translator.InsertQueryMap), args.Error(1)
}

func (m *MockTranslator) TranslateDeleteQuerytoBigtable(query string) (*translator.DeleteQueryMap, error) {
	args := m.Called(query)
	return args.Get(0).(*translator.DeleteQueryMap), args.Error(1)
}
func (m *MockTranslator) TranslateUpdateQuerytoBigtable(query string) (*translator.UpdateQueryMap, error) {
	args := m.Called(query)
	return args.Get(0).(*translator.UpdateQueryMap), args.Error(1)
}

/*
	func TestHandleQuery(t *testing.T) {
		mockSender := &mockSender{}
		mockProxy := &Proxy{
			tableConfig: tableConfigs,
			translator:  &translator.Translator{TableConfig: tableConfigs},
			logger:      zap.NewNop(),
			ctx:         context.Background(),
			//bClient:     mockBigtableClient,
			bClient: nil,
			otelInst: &otelgo.OpenTelemetry{Config: &otelgo.OTelConfig{
				ServiceName: "test",
				OTELEnabled: false,
			}},
		}

		type fields struct {
			ctx                 context.Context
			proxy               *Proxy
			conn                *proxycore.Conn
			keyspace            string
			preparedSystemQuery map[[16]byte]interface{}
			preparedQuerys      map[[16]byte]interface{}
		}

		// selectQuery := &partialQuery{
		// 	query: "SELECT * FROM test_keyspace.test_table WHERE test_id = '123",
		// }

		insertQuery := &partialQuery{
			query: "INSERT INTO test_keyspace.test_table (test_id, test_hash) VALUES ('asd', 'asd-asd-asd-asd')",
		}

		deleteQuery := &partialQuery{
			query: "DELETE FROM test_keyspace.test_table WHERE test_id = '123'",
		}

		tests := []struct {
			name    string
			fields  fields
			c       *client
			raw     *frame.RawFrame
			query   *partialQuery
			wantErr bool
			setup   func(*client)
		}{
			// {
			// 	name:    "Select Query - Success",
			// 	raw:     mockRawFrame,
			// 	query:   selectQuery,
			// 	wantErr: false,
			// 	fields: fields{
			// 		proxy: mockProxy,
			// 	},
			// 	setup: func(c *client) {
			// 		c.preparedSystemQuery = make(map[[16]byte]interface{})
			// 		c.preparedQuerys = make(map[[16]byte]interface{})
			// 	},
			// },
			{
				name:    "Insert Query - Success",
				raw:     mockRawFrame,
				query:   insertQuery,
				wantErr: false,
				fields: fields{
					proxy: mockProxy,
				},
				setup: func(c *client) {
					c.preparedSystemQuery = make(map[[16]byte]interface{})
					c.preparedQuerys = make(map[[16]byte]interface{})
				},
			},
			{
				name:    "Delete Query - Success",
				raw:     mockRawFrame,
				query:   deleteQuery,
				wantErr: false,
				fields: fields{
					proxy: mockProxy,
				},
				setup: func(c *client) {
					c.preparedSystemQuery = make(map[[16]byte]interface{})
					c.preparedQuerys = make(map[[16]byte]interface{})
				},
			},
		}

		for _, tt := range tests {
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               mockProxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				preparedQuerys:      tt.fields.preparedQuerys,
				sender:              mockSender,
			}
			t.Run(tt.name, func(t *testing.T) {
				mockSender.SendCalled = false
				c.handleQuery(tt.raw, tt.query)

				if tt.wantErr {
					if mockSender.SendCalled {
						t.Errorf("Send should not have been called for test: %s", tt.name)
					}
				} else {
					if !mockSender.SendCalled {
						t.Errorf("Send was not called when expected for test: %s", tt.name)
					}
				}
			})
		}
	}
*/
type mockQuery struct{}

func TestHandleExecute(t *testing.T) {
	mockSender := &mockSender{}
	mockPartialExecute := &partialExecute{
		PositionalValues: positionValues,
		NamedValues:      namedValues,
	}
	mockSystemQueryID := preparedIdKey([]byte("some_prepared_query_id"))

	tests := []struct {
		name           string
		client         *client
		raw            *frame.RawFrame
		msg            *partialExecute
		setupClient    func(c *client)
		wantSendCalled bool
		wantSendMsg    message.Message
	}{
		{
			name: "System Query Execution",
			raw:  mockRawFrame,
			msg:  &partialExecute{queryId: []byte("some_prepared_query_id")},
			setupClient: func(c *client) {
				c.preparedSystemQuery = map[[16]byte]interface{}{
					mockSystemQueryID: &mockQuery{},
				}
			},
			wantSendCalled: true,
		},
		{
			name: "Prepared Query Execution",
			raw:  mockRawFrame,
			msg:  &partialExecute{queryId: []byte("some_other_prepared_query_id")},
			setupClient: func(c *client) {
				c.preparedQuerys = map[[16]byte]interface{}{
					preparedIdKey([]byte("some_other_prepared_query_id")): &translator.SelectQueryMap{},
				}
			},
			wantSendCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				proxy:               mockProxy,
				preparedSystemQuery: make(map[[16]byte]interface{}),
				preparedQuerys:      make(map[[16]byte]interface{}),
				sender:              mockSender,
			}
			if tt.setupClient != nil {
				tt.setupClient(c)
			}

			c.handleExecute(mockRawFrame, mockPartialExecute)
			if tt.wantSendCalled {
				if !mockSender.SendCalled {
					t.Errorf("Send was not called when expected for test: %s", tt.name)
				}
			} else {
				if mockSender.SendCalled {
					t.Errorf("Send was called when not expected for test: %s", tt.name)
				}
			}
		})
	}
}

func Test_client_handlePrepare(t *testing.T) {
	mockSender := &mockSender{}
	selectQuery := &message.Prepare{
		Query: "SELECT * FROM system.local WHERE key = 'local'",
	}
	useQuery := &message.Prepare{
		Query: "USE test_keyspace",
	}
	invalidQuery := &message.Prepare{
		Query: "INVALID QUERY",
	}

	tests := []struct {
		name           string
		client         *client
		raw            *frame.RawFrame
		msg            *message.Prepare
		setupClient    func(c *client)
		wantSendCalled bool
		wantSendMsg    message.Message
	}{
		{
			name: "Select Query",
			raw:  mockRawFrame,
			msg:  selectQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.preparedQuerys = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
		},
		{
			name: "Use Query",
			raw:  mockRawFrame,
			msg:  useQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.preparedQuerys = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
		},
		{
			name: "Invalid Query",
			raw:  mockRawFrame,
			msg:  invalidQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.preparedQuerys = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
		},
		{
			name: "Select Statement with Invalid Table",
			raw:  mockRawFrame,
			msg:  selectQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.preparedQuerys = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
			wantSendMsg:    &message.Invalid{},
		},
		{
			name: "Query Parsing Error",
			raw:  mockRawFrame,
			msg:  &message.Prepare{Query: "invalid query syntax"},
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.preparedQuerys = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
			wantSendMsg:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				proxy:               mockProxy,
				preparedSystemQuery: make(map[[16]byte]interface{}),
				preparedQuerys:      make(map[[16]byte]interface{}),
				sender:              mockSender,
			}
			if tt.setupClient != nil {
				tt.setupClient(c)
			}

			c.handlePrepare(tt.raw, tt.msg)
			if tt.wantSendCalled {
				if !mockSender.SendCalled {
					t.Errorf("Send was not called when expected for test: %s", tt.name)
				}
			} else {
				if mockSender.SendCalled {
					t.Errorf("Send was called when not expected for test: %s", tt.name)
				}
			}
		})
	}
}

func createTableConfig() *tableConfig.TableConfig {
	return &tableConfig.TableConfig{
		TablesMetaData:  mockTableConfig,
		PkMetadataCache: mockPkMetadata,
	}
}

var mockTableConfig = map[string]map[string]map[string]*tableConfig.Column{

	"keyspace": {
		"test_table": {
			"test_id": {
				ColumnName: "test_id",
				ColumnType: "text",
				Metadata: message.ColumnMetadata{
					Keyspace: "",
					Table:    "test_table",
					Name:     "test_id",
					Type:     datatype.Varchar,
					Index:    0,
				},
				IsPrimaryKey: true,
			},
			"test_hash": {
				ColumnName: "test_hash",
				ColumnType: "text",
				Metadata: message.ColumnMetadata{
					Keyspace: "",
					Table:    "test_table",
					Name:     "test_hash",
					Type:     datatype.Varchar,
					Index:    1,
				},
			},

			"column1": &tableConfig.Column{
				ColumnName:   "column1",
				ColumnType:   "text",
				IsPrimaryKey: true,
				PkPrecedence: 1,
			},
			"column2": &tableConfig.Column{
				ColumnName:   "column2",
				ColumnType:   "blob",
				IsPrimaryKey: false,
			},
			"column3": &tableConfig.Column{
				ColumnName:   "column3",
				ColumnType:   "boolean",
				IsPrimaryKey: false,
			},

			"column10": &tableConfig.Column{
				ColumnName:   "column10",
				ColumnType:   "text",
				IsPrimaryKey: true,
				PkPrecedence: 2,
			},
		},
		"user_info": {
			"name": &tableConfig.Column{
				ColumnName:   "name",
				ColumnType:   "text",
				IsPrimaryKey: true,
				PkPrecedence: 0,
				IsCollection: false,
			},
		},
	},
}

var mockPkMetadata = map[string]map[string][]tableConfig.Column{
	"keyspace": {
		"test_table": {
			{
				ColumnName:   "test_id",
				CQLType:      "text",
				IsPrimaryKey: true,
				PkPrecedence: 1,
			},
			{
				ColumnName:   "test_hash",
				ColumnType:   "text",
				IsPrimaryKey: false,
				PkPrecedence: 2,
			},
		},
		"user_info": {
			{
				ColumnName:   "name",
				CQLType:      "text",
				IsPrimaryKey: true,
				PkPrecedence: 0,
			},
		},
	},
}

func Test_validatePrimaryKey(t *testing.T) {
	type args struct {
		query *translator.InsertQueryMap
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Primary key present and not null",
			args: args{
				query: &translator.InsertQueryMap{
					Columns: []translator.Column{
						{Name: "id", IsPrimaryKey: true},
						{Name: "name", IsPrimaryKey: false},
					},
					Values: []interface{}{[]uint8{0, 0, 1}, "Alice"},
				},
			},
			want: "",
		},
		{
			name: "Primary key missing value",
			args: args{
				query: &translator.InsertQueryMap{
					Columns: []translator.Column{
						{Name: "id", IsPrimaryKey: true},
						{Name: "name", IsPrimaryKey: false},
					},
					Values: []interface{}{nil, "Bob"},
				},
			},
			want: "id",
		},
		{
			name: "No primary key column",
			args: args{
				query: &translator.InsertQueryMap{
					Columns: []translator.Column{
						{Name: "age", IsPrimaryKey: false},
						{Name: "name", IsPrimaryKey: false},
					},
					Values: []interface{}{25, "Eve"},
				},
			},
			want: "",
		},
		{
			name: "Multiple primary keys, one missing value",
			args: args{
				query: &translator.InsertQueryMap{
					Columns: []translator.Column{
						{Name: "user_id", IsPrimaryKey: true},
						{Name: "product_id", IsPrimaryKey: true},
					},
					Values: []interface{}{nil, 101},
				},
			},
			want: "user_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validatePrimaryKey(tt.args.query); got != tt.want {
				t.Errorf("validatePrimaryKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
