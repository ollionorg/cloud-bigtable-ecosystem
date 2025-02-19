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
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	rh "github.com/ollionorg/cassandra-to-bigtable-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/tableConfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func TestSelect_ConstructRequestValues(t *testing.T) {
	tests := []struct {
		name      string
		value     interface{}
		expected  *btpb.Value
		expectErr bool
	}{
		{
			name:  "String value",
			value: "test-string",
			expected: &btpb.Value{
				Kind: &btpb.Value_BytesValue{BytesValue: []byte("test-string")},
				Type: &btpb.Type{Kind: &btpb.Type_BytesType{}},
			},
			expectErr: false,
		},
		{
			name:  "Int64 value",
			value: int64(42),
			expected: &btpb.Value{
				Kind: &btpb.Value_IntValue{IntValue: 42},
				Type: &btpb.Type{Kind: &btpb.Type_Int64Type{}},
			},
			expectErr: false,
		},
		{
			name:  "Float64 value",
			value: 3.14,
			expected: &btpb.Value{
				Kind: &btpb.Value_FloatValue{FloatValue: 3.14},
				Type: &btpb.Type{Kind: &btpb.Type_Float64Type{}},
			},
			expectErr: false,
		},
		{
			name:  "Array value",
			value: []interface{}{"test1"},
			expected: &btpb.Value{
				Kind: &btpb.Value_ArrayValue{
					ArrayValue: &btpb.ArrayValue{
						Values: []*btpb.Value{
							{
								Kind: &btpb.Value_BytesValue{BytesValue: []byte("test1")},
								Type: &btpb.Type{Kind: &btpb.Type_BytesType{}},
							},
						},
					},
				},
				Type: &btpb.Type{Kind: &btpb.Type_ArrayType{ArrayType: &btpb.Type_Array{ElementType: &btpb.Type{Kind: &btpb.Type_BytesType{}}}}},
			},
			expectErr: false,
		},
		{
			name:      "Unsupported type",
			value:     map[string]string{"key": "value"},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := constructRequestValues(tt.value)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestRequestParamsHeaderValue(t *testing.T) {
	tests := []struct {
		instance   string
		appProfile string
		expected   string
	}{
		{
			instance:   "my-instance",
			appProfile: "my-app-profile",
			expected:   "name=my-instance&app_profile_id=my-app-profile",
		},
		{
			instance:   "",
			appProfile: "",
			expected:   "name=&app_profile_id=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.instance+"_"+tt.appProfile, func(t *testing.T) {
			result := requestParamsHeaderValue(tt.instance, tt.appProfile)
			assert.Equal(t, tt.expected, result, "Formatted header value should match expected output")
		})
	}
}

func TestConstructRequestParams_Success(t *testing.T) {
	tests := []struct {
		name      string
		value     map[string]interface{}
		expected  map[string]*btpb.Value
		expectErr bool
	}{
		{
			name: "String value",
			value: map[string]interface{}{
				"param": "test-string",
			},
			expected: map[string]*btpb.Value{"param": {
				Kind: &btpb.Value_BytesValue{BytesValue: []byte("test-string")},
				Type: &btpb.Type{Kind: &btpb.Type_BytesType{}},
			}},
			expectErr: false,
		},
		{
			name: "Unsupported type",
			value: map[string]interface{}{
				"param1": map[string]string{"key": "value"}, // unsupported type for testing
			},
			expected:  nil,
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := constructRequestParams(tt.value)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSelectStatement(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name                              string
		query                             rh.QueryMetadata
		mockGetRows                       func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error)
		mockBuildMetadata                 func(rowMap map[string]map[string]interface{}, query rh.QueryMetadata) ([]*message.ColumnMetadata, []string, error)
		mockBuildResponseRow              func(rowMap map[string]interface{}, query rh.QueryMetadata, cmd []*message.ColumnMetadata, mapKeyArray []string) (message.Row, error)
		mockGetMetadataForSelectedColumns func(tableName string, selectedColumns []tableConfig.SelectedColumns, keyspaceName string) ([]*message.ColumnMetadata, error)
		expectedResult                    *message.RowsResult
		expectedDuration                  time.Duration
		expectedError                     error
	}{
		{
			name: "successful query",
			query: rh.QueryMetadata{
				KeyspaceName: "test-instance",
				TableName:    "test-table",
				SelectedColumns: []tableConfig.SelectedColumns{
					{Name: "col1", FormattedColumn: "col1"},
					{Name: "col2", FormattedColumn: "col2"},
				},
				Params: map[string]interface{}{},
			},
			mockGetRows: func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
				return map[string]map[string]interface{}{
					"row1": {"col1": "value1", "col2": "value2"},
				}, nil
			},
			mockBuildMetadata: func(rowMap map[string]map[string]interface{}, query rh.QueryMetadata) ([]*message.ColumnMetadata, []string, error) {
				return []*message.ColumnMetadata{
					{Name: "col1", Type: datatype.Varchar, Index: 0},
					{Name: "col2", Type: datatype.Varchar, Index: 1},
				}, []string{"col1", "col2"}, nil
			},
			mockBuildResponseRow: func(rowMap map[string]interface{}, query rh.QueryMetadata, cmd []*message.ColumnMetadata, mapKeyArray []string) (message.Row, error) {
				return message.Row{
					[]byte("value1"),
					[]byte("value2"),
				}, nil
			},
			expectedResult: &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: 2,
					Columns: []*message.ColumnMetadata{
						{Name: "col1", Type: datatype.Varchar, Index: 0},
						{Name: "col2", Type: datatype.Varchar, Index: 1},
					},
				},
				Data: message.RowSet{
					{
						[]byte("value1"),
						[]byte("value2"),
					},
				},
			},
			expectedDuration: time.Duration(0), // Mocked duration
			expectedError:    nil,
		},
		{
			name: "error in ExecuteBigtableQuery",
			query: rh.QueryMetadata{
				KeyspaceName: "test-instance",
				TableName:    "test-table",
			},
			mockGetRows: func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
				return nil, fmt.Errorf("mock error in ExecuteBigtableQuery")
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("mock error in ExecuteBigtableQuery"),
		},
		{
			name: "error in BuildMetadata",
			query: rh.QueryMetadata{
				KeyspaceName: "test-instance",
				TableName:    "test-table",
			},
			mockGetRows: func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
				return map[string]map[string]interface{}{
					"row1": {"col1": "value1"},
				}, nil
			},
			mockBuildMetadata: func(rowMap map[string]map[string]interface{}, query rh.QueryMetadata) ([]*message.ColumnMetadata, []string, error) {
				return nil, nil, fmt.Errorf("mock error in BuildMetadata")
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("mock error in BuildMetadata"),
		},
		{
			name: "error in BuildResponseRow",
			query: rh.QueryMetadata{
				KeyspaceName: "test-instance",
				TableName:    "test-table",
			},
			mockGetRows: func(resp *btpb.ExecuteQueryResponse_Results, cfs []*btpb.ColumnMetadata, query rh.QueryMetadata, rowCount *int, rowMapData map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
				return map[string]map[string]interface{}{
					"row1": {"col1": "value1"},
				}, nil
			},
			mockBuildMetadata: func(rowMap map[string]map[string]interface{}, query rh.QueryMetadata) ([]*message.ColumnMetadata, []string, error) {
				return []*message.ColumnMetadata{
					{Name: "col1", Type: datatype.Varchar},
				}, []string{"col1"}, nil
			},
			mockBuildResponseRow: func(rowMap map[string]interface{}, query rh.QueryMetadata, cmd []*message.ColumnMetadata, mapKeyArray []string) (message.Row, error) {
				return nil, fmt.Errorf("mock error in BuildResponseRow")
			},
			expectedResult: nil,
			expectedError:  fmt.Errorf("mock error in BuildResponseRow"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Mock dependencies
			mockResponseHandler := &MockResponseHandler{
				mockBuildMetadata:    tc.mockBuildMetadata,
				mockBuildResponseRow: tc.mockBuildResponseRow,
				mockGetRows:          tc.mockGetRows,
			}

			mockStream := &MockExecuteQueryStream{
				Responses: []interface{}{
					&btpb.ExecuteQueryResponse{
						Response: &btpb.ExecuteQueryResponse_Metadata{},
					},
					&btpb.ExecuteQueryResponse{
						Response: &btpb.ExecuteQueryResponse_Results{},
					},
					io.EOF,
				},
			}

			mockClient := &MockBigtableSQLClient{
				mockExecuteQuery: func(ctx context.Context, req *btpb.ExecuteQueryRequest, opts ...grpc.CallOption) (btpb.Bigtable_ExecuteQueryClient, error) {
					return mockStream, nil
				},
			}
			btc := NewBigtableClient(map[string]*bigtable.Client{"test-instance": {}}, zap.NewNop(), mockClient, BigTableConfig{}, mockResponseHandler, nil, nil)

			result, _, err := btc.SelectStatement(context.Background(), tc.query)

			// Validate error
			if tc.expectedError != nil {
				if err == nil || err.Error() != tc.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tc.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Validate result
			if !rowsResultEqual(result, tc.expectedResult) {
				t.Errorf("expected result %v, got %v", tc.expectedResult, result)
			}

			// Validate duration (mocked, so no strict checks)
			// if duration < 0 {
			// 	t.Errorf("invalid duration: %v", duration)
			// }
		})
	}
}
