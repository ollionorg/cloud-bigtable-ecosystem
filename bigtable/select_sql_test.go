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
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/datastax/go-cassandra-native-protocol/message"
	rh "github.com/ollionorg/cassandra-to-bigtable-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/tableConfig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MockBigtableSQLClient struct {
	btpb.BigtableClient
	mockExecuteQuery func(ctx context.Context, req *btpb.ExecuteQueryRequest, opts ...grpc.CallOption) (btpb.Bigtable_ExecuteQueryClient, error)
}

func (m *MockBigtableSQLClient) ExecuteQuery(ctx context.Context, req *btpb.ExecuteQueryRequest, opts ...grpc.CallOption) (btpb.Bigtable_ExecuteQueryClient, error) {
	return m.mockExecuteQuery(ctx, req, opts...)
}

type MockExecuteQueryStream struct {
	grpc.ClientStream
	Responses []interface{} // List of responses to send
	index     int           // Current response index
}

func (m *MockExecuteQueryStream) Recv() (*btpb.ExecuteQueryResponse, error) {
	if m.index >= len(m.Responses) {
		return nil, io.EOF
	}

	response := m.Responses[m.index]
	m.index++

	switch r := response.(type) {
	case *btpb.ExecuteQueryResponse:
		return r, nil
	case error:
		return nil, r
	default:
		return nil, fmt.Errorf("invalid response type")
	}
}

type MockResponseHandler struct {
	rh.ResponseHandlerIface
	mockGetRows          func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error)
	mockBuildMetadata    func(rowMap map[string]map[string]interface{}, query rh.QueryMetadata) ([]*message.ColumnMetadata, []string, error)
	mockBuildResponseRow func(rowMap map[string]interface{}, query rh.QueryMetadata, cmd []*message.ColumnMetadata, mapKeyArray []string) (message.Row, error)
}

func (m *MockResponseHandler) GetRows(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {

	return m.mockGetRows(resp, cfs, query, rowCount, rowMapData)
}
func (m *MockResponseHandler) BuildMetadata(rowMap map[string]map[string]interface{}, query rh.QueryMetadata) ([]*message.ColumnMetadata, []string, error) {
	return m.mockBuildMetadata(rowMap, query)
}
func (m *MockResponseHandler) BuildResponseRow(rowMap map[string]interface{}, query rh.QueryMetadata, cmd []*message.ColumnMetadata, mapKeyArray []string) (message.Row, error) {
	return m.mockBuildResponseRow(rowMap, query, cmd, mapKeyArray)
}

func TestExecuteBigtableQuery(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name           string
		query          rh.QueryMetadata
		mockMetadata   *btpb.ExecuteQueryResponse
		mockResults    *btpb.ExecuteQueryResponse
		mockGetRows    func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error)
		expectedResult map[string]map[string]interface{}
		expectedError  error
	}{
		{
			name: "successful query",
			query: rh.QueryMetadata{
				KeyspaceName: "fake-instance",
				Query:        "SELECT * FROM table",
				Params:       map[string]interface{}{},
			},
			mockMetadata: &btpb.ExecuteQueryResponse{
				Response: &btpb.ExecuteQueryResponse_Metadata{
					Metadata: &btpb.ResultSetMetadata{
						Schema: &btpb.ResultSetMetadata_ProtoSchema{
							ProtoSchema: &btpb.ProtoSchema{
								Columns: []*btpb.ColumnMetadata{
									{Name: "_key"},
									{Name: "cf1"},
									{Name: "extra_info"},
									{Name: "tags"},
								},
							},
						},
					},
				},
			},
			mockResults: &btpb.ExecuteQueryResponse{
				Response: &btpb.ExecuteQueryResponse_Results{
					Results: &btpb.PartialResultSet{
						PartialRows: &btpb.PartialResultSet_ProtoRowsBatch{
							ProtoRowsBatch: &btpb.ProtoRowsBatch{
								BatchData: []byte("\x12\n\x12\b33#Jenny\x12\xc6\x01\"\xc3\x01\n\x15\"\x13\n\x05\x12\x03age\n\n\x12\b\x00\x00\x00\x00\x00\x00\x00!\n\x15\"\x13\n\t\x12\abalance\n\x06\x12\x04G\x83\x87\xd6\n\x1c\"\x1a\n\f\x12\nbirth_date\n\n\x12\b\x00\x00\x01\x8f\xc9\tw\xc0\n\x12\"\x10\n\x06\x12\x04code\n\x06\x12\x04\x00\x00\x04\xd2\n\x1a\"\x18\n\n\x12\bcredited\n\n\x12\b@È@\x00\x00\x00\x00\n\x14\"\x12\n\v\x12\tis_active\n\x03\x12\x01\x01\n\x13\"\x11\n\x06\x12\x04name\n\a\x12\x05Jenny\n\x1a\"\x18\n\n\x12\bzip_code\n\n\x12\b\x00\x00\x00\x00\x00\x06\xef\xfe\x12/\"-\n\x13\"\x11\n\x06\x12\x04make\n\a\x12\x05Tesla\n\x16\"\x14\n\a\x12\x05model\n\t\x12\aModel S\x12\"\" \n\x0f\"\r\n\a\x12\x05Black\n\x02\x12\x00\n\r\"\v\n\x05\x12\x03Red\n\x02\x12\x00"),
							},
						},
					},
				},
			},
			mockGetRows: func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
				return map[string]map[string]interface{}{
					"row1": {
						"_key":       "33",
						"cf1":        "Jenny",
						"extra_info": "value",
						"tags":       "value",
					},
				}, nil
			},
			expectedResult: map[string]map[string]interface{}{
				"row1": {
					"_key":       "33",
					"cf1":        "Jenny",
					"extra_info": "value",
					"tags":       "value",
				},
			},
			expectedError: nil,
		},
		{
			name: "query with invalid keyspace",
			query: rh.QueryMetadata{
				KeyspaceName: "invalid-instance",
				Query:        "SELECT * FROM table",
				Params:       map[string]interface{}{},
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("invalid keySpace"),
		},
		{
			name: "query with GetRows error",
			query: rh.QueryMetadata{
				KeyspaceName: "fake-instance",
				Query:        "SELECT * FROM table",
				Params:       map[string]interface{}{},
			},
			mockMetadata: &btpb.ExecuteQueryResponse{
				Response: &btpb.ExecuteQueryResponse_Metadata{
					Metadata: &btpb.ResultSetMetadata{
						Schema: &btpb.ResultSetMetadata_ProtoSchema{
							ProtoSchema: &btpb.ProtoSchema{
								Columns: []*btpb.ColumnMetadata{
									{Name: "_key"},
									{Name: "cf1"},
									{Name: "extra_info"},
									{Name: "tags"},
								},
							},
						},
					},
				},
			},
			mockResults: &btpb.ExecuteQueryResponse{
				Response: &btpb.ExecuteQueryResponse_Results{
					Results: &btpb.PartialResultSet{
						PartialRows: &btpb.PartialResultSet_ProtoRowsBatch{
							ProtoRowsBatch: &btpb.ProtoRowsBatch{
								BatchData: []byte("\x12\n\x12\b33#Jenny\x12\xc6\x01\"\xc3\x01\n\x15\"\x13\n\x05\x12\x03age\n\n\x12\b\x00\x00\x00\x00\x00\x00\x00!"),
							},
						},
					},
				},
			},
			mockGetRows: func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
				return nil, fmt.Errorf("GetRows error")
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("GetRows error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Mock response handler
			mockResponseHandler := &MockResponseHandler{
				mockGetRows: tc.mockGetRows,
			}

			// Mock BigtableClient
			mockStream := &MockExecuteQueryStream{
				Responses: []interface{}{
					tc.mockMetadata,
					tc.mockResults,
					io.EOF,
				},
			}

			mockClient := &MockBigtableSQLClient{
				mockExecuteQuery: func(ctx context.Context, req *btpb.ExecuteQueryRequest, opts ...grpc.CallOption) (btpb.Bigtable_ExecuteQueryClient, error) {
					return mockStream, nil
				},
			}
			btc := NewBigtableClient(map[string]*bigtable.Client{"fake-instance": {}}, zap.NewNop(), mockClient, BigTableConfig{}, mockResponseHandler, nil, nil)

			// Call the function
			result, err := btc.ExecuteBigtableQuery(context.Background(), tc.query)

			// Validate error
			if tc.expectedError != nil {
				if err == nil || err.Error() != tc.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tc.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Validate result
			if !reflect.DeepEqual(result, tc.expectedResult) {
				t.Errorf("expected result %v, got %v", tc.expectedResult, result)
			}
		})
	}
}

func TestSortRowsResultBySelectedColumns(t *testing.T) {
	tests := []struct {
		name            string
		input           *message.RowsResult
		selectedColumns []tableConfig.SelectedColumns
		expected        *message.RowsResult
	}{
		{
			name: "Sort using primary column names",
			input: &message.RowsResult{
				Metadata: &message.RowsMetadata{
					Columns: []*message.ColumnMetadata{
						{Name: "col2", Index: 1},
						{Name: "col1", Index: 0},
					},
				},
				Data: message.RowSet{
					{[]byte("value1_2"), []byte("value1_1")},
					{[]byte("value2_2"), []byte("value2_1")},
				},
			},
			selectedColumns: []tableConfig.SelectedColumns{
				{Name: "col1"},
				{Name: "col2"},
			},
			expected: &message.RowsResult{
				Metadata: &message.RowsMetadata{
					Columns: []*message.ColumnMetadata{
						{Name: "col1", Index: 0},
						{Name: "col2", Index: 1},
					},
				},
				Data: message.RowSet{
					{[]byte("value1_2"), []byte("value1_1")},
					{[]byte("value2_2"), []byte("value2_1")},
				},
			},
		},
		// {
		// 	name: "Sort using alias",
		// 	input: &message.RowsResult{
		// 		Metadata: &message.RowsMetadata{
		// 			Columns: []*message.ColumnMetadata{
		// 				{Name: "colA", Index: 0},
		// 				{Name: "colB", Index: 1},
		// 			},
		// 		},
		// 		Data: message.RowSet{
		// 			{[]byte("value1_A"), []byte("value1_B")},
		// 			{[]byte("value2_A"), []byte("value2_B")},
		// 		},
		// 	},
		// 	selectedColumns: []tableConfig.SelectedColumns{
		// 		{Name: "colB", Alias: "aliasB"},
		// 		{Name: "colA", Alias: "aliasA"},
		// 	},
		// 	expected: &message.RowsResult{
		// 		Metadata: &message.RowsMetadata{
		// 			Columns: []*message.ColumnMetadata{
		// 				{Name: "colB", Index: 1},
		// 				{Name: "colA", Index: 0},
		// 			},
		// 		},
		// 		Data: message.RowSet{
		// 			{[]byte("value1_B"), []byte("value1_A")},
		// 			{[]byte("value2_B"), []byte("value2_A")},
		// 		},
		// 	},
		// },
		// {
		// 	name: "No selected columns - no sorting",
		// 	input: &message.RowsResult{
		// 		Metadata: &message.RowsMetadata{
		// 			Columns: []*message.ColumnMetadata{
		// 				{Name: "col1", Index: 0},
		// 				{Name: "col2", Index: 1},
		// 			},
		// 		},
		// 		Data: message.RowSet{
		// 			{[]byte("value1_1"), []byte("value1_2")},
		// 			{[]byte("value2_1"), []byte("value2_2")},
		// 		},
		// 	},
		// 	selectedColumns: []tableConfig.SelectedColumns{},
		// 	expected: &message.RowsResult{
		// 		Metadata: &message.RowsMetadata{
		// 			Columns: []*message.ColumnMetadata{
		// 				{Name: "col1", Index: 0},
		// 				{Name: "col2", Index: 1},
		// 			},
		// 		},
		// 		Data: message.RowSet{
		// 			{[]byte("value1_1"), []byte("value1_2")},
		// 			{[]byte("value2_1"), []byte("value2_2")},
		// 		},
		// 	},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sortRowsResultBySelectedColumns(tt.input, tt.selectedColumns)
			if !rowsResultEqual(result, tt.expected) {
				t.Errorf("got %+v, expected %+v", result, tt.expected)
			}

		})
	}
}
func rowsResultEqual(a, b *message.RowsResult) bool {
	if a == nil || b == nil {
		return a == b
	}

	if len(a.Metadata.Columns) != len(b.Metadata.Columns) ||
		len(a.Data) != len(b.Data) {
		return false
	}

	for i := range a.Metadata.Columns {
		if a.Metadata.Columns[i].Name != b.Metadata.Columns[i].Name ||
			a.Metadata.Columns[i].Index != b.Metadata.Columns[i].Index {
			return false
		}
	}

	for i := range a.Data {
		if len(a.Data[i]) != len(b.Data[i]) {
			return false
		}
		for j := range a.Data[i] {
			if !reflect.DeepEqual(a.Data[i][j], b.Data[i][j]) {
				return false
			}
		}
	}

	return true
}
