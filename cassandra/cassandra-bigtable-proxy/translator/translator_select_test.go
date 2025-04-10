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
package translator

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
	cql "github.com/ollionorg/cassandra-to-bigtable-proxy/third_party/cqlparser"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestTranslator_TranslateSelectQuerytoBigtable(t *testing.T) {
	type fields struct {
		Logger *zap.Logger
	}
	type args struct {
		query string
	}

	inputRawQuery := `select column1, column2, column3 from  test_keyspace.test_table
	where column1 = 'test' AND column3='true'
	AND column5 <= '2015-05-03 13:30:54.234' AND column6 >= '123'
	AND column9 > '-10000000' LIMIT 20000;;`
	timeStamp, _ := parseTimestamp("2015-05-03 13:30:54.234")

	inputPreparedQuery := `select column1, column2, column3 from  test_keyspace.test_table
	where column1 = '?' AND column2='?' AND column3='?'
	AND column5='?' AND column6='?'
	AND column9='?';`
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *SelectQueryMap
		wantErr bool
	}{
		{
			name: "Writetime Query without as keyword",
			args: args{
				query: `select column1, WRITETIME(column2) from test_keyspace.test_table where column1 = 'test';`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				Query:           "SELECT column1,WRITE_TIMESTAMP(cf1, 'column2') FROM test_table WHERE column1 = @value1;",
				QueryType:       "select",
				TranslatedQuery: "SELECT column1,WRITE_TIMESTAMP(cf1, 'column2') FROM test_table WHERE column1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				Limit: Limit{
					IsLimit: false,
				},
				OrderBy: OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
			},
		},
		{
			name: "Writetime Query with as keyword",
			args: args{
				query: `select column1, WRITETIME(column2) as name from test_keyspace.test_table where column1 = 'test';`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				Query:           "SELECT column1,WRITE_TIMESTAMP(cf1, 'column2') as name FROM test_table WHERE column1 = @value1;",
				QueryType:       "select",
				TranslatedQuery: "SELECT column1,WRITE_TIMESTAMP(cf1, 'column2') as name FROM test_table WHERE column1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				Limit: Limit{
					IsLimit: false,
				},
				OrderBy: OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
			},
		},
		{
			name: "As Query",
			args: args{
				query: `select column2 as name from test_keyspace.test_table where column1 = 'test';`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				Query:           "select column2 as name from test_keyspace.test_table where column1 = 'test'",
				QueryType:       "select",
				TranslatedQuery: "SELECT cf1['column2'] as name FROM test_table WHERE column1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				Limit: Limit{
					IsLimit: false,
				},
				OrderBy: OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
			},
		},
		{
			name: "Query without Columns",
			args: args{
				query: `select from key_space.test_table where column1 = 'test'`,
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "test for raw query when column name not exist in schema mapping table",
			args: args{
				query: "select column101, column2, column3 from  key_space.test_table where column1 = 'test' AND column3='true' AND column5 <= '2015-05-03 13:30:54.234' AND column6 >= '123' AND column9 > '-10000000' LIMIT 20000;",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Query without keyspace name",
			args: args{
				query: `select column1, column2 from test_table where column1 = '?' and column1 in ('?', '?');`,
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "MALFORMED QUERY",
			args: args{
				query: "MALFORMED QUERY",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "test for raw query success",
			args: args{
				query: inputRawQuery,
			},
			want: &SelectQueryMap{
				Query:           inputRawQuery,
				QueryType:       "select",
				TranslatedQuery: "SELECT column1,cf1['column2'],cf1['column3'] FROM test_table WHERE column1 = @value1 AND TO_INT64(cf1['column3']) = @value2 AND TO_TIME(cf1['column5']) <= @value3 AND TO_INT64(cf1['column6']) >= @value4 AND TO_INT64(cf1['column9']) > @value5 LIMIT 20000;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:   "column3",
						Operator: "=",
						Value:    "@value2",
					},
					{
						Column:   "column5",
						Operator: "<=",
						Value:    "@value3",
					},
					{
						Column:   "column6",
						Operator: ">=",
						Value:    "@value4",
					},
					{
						Column:   "column9",
						Operator: ">",
						Value:    "@value5",
					},
				},
				Limit: Limit{
					IsLimit: true,
					Count:   "2000",
				},
				OrderBy: OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
					"value2": true,
					"value3": timeStamp,
					"value4": int32(123),
					"value5": int64(-10000000),
				},
				ParamKeys: []string{"value1", "value2", "value3", "value4", "value5"},
			},
			wantErr: false,
		},
		{
			name: "test for prepared query success",
			args: args{
				query: inputPreparedQuery,
			},
			want: &SelectQueryMap{
				Query:           inputPreparedQuery,
				QueryType:       "select",
				TranslatedQuery: "SELECT column1,cf1['column2'],cf1['column3'] FROM test_table WHERE column1 = @value1 AND TO_BLOB(cf1['column2']) = @value2 AND TO_INT64(cf1['column3']) = @value3 AND TO_TIME(cf1['column5']) = @value4 AND TO_INT64(cf1['column6']) = @value5 AND TO_INT64(cf1['column9']) = @value6;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:   "column2",
						Operator: "=",
						Value:    "@value2",
					},
					{
						Column:   "column3",
						Operator: "=",
						Value:    "@value3",
					},
					{
						Column:   "column5",
						Operator: "=",
						Value:    "@value4",
					},
					{
						Column:   "column6",
						Operator: "=",
						Value:    "@value5",
					},
					{
						Column:   "column9",
						Operator: "=",
						Value:    "@value6",
					},
				},
				Params: map[string]interface{}{},
				Limit: Limit{
					IsLimit: true,
					Count:   "2000",
				},
				OrderBy: OrderBy{
					IsOrderBy: false,
				},
				ParamKeys: []string{"value1", "value2", "value3", "value4", "value5", "value6"},
			},
			wantErr: false,
		},
		{
			name: "test for query without clause success",
			args: args{
				query: `select column1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT 20000;`,
			},
			want: &SelectQueryMap{
				Query:           `select column1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT 20000;`,
				QueryType:       "select",
				TranslatedQuery: "SELECT column1,cf1['column2'],cf1['column3'] FROM test_table ORDER BY column1 asc LIMIT 20000;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				ColumnMeta: ColumnMeta{
					Star:   false,
					Column: []schemaMapping.SelectedColumns{{Name: "column1"}, {Name: "column2"}, {Name: "column3"}},
				},
				Limit: Limit{
					IsLimit: true,
					Count:   "2000",
				},
				OrderBy: OrderBy{
					IsOrderBy: true,
					Column:    "column1",
					Operation: "asc",
				},
			},
			wantErr: false,
		},
		{
			name: "error at Column parsing",
			args: args{
				query: "select  from table;",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error at Clause parsing when column type not found",
			args: args{
				query: "select * from test_keyspace.table_name where name=test;",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error at Clause parsing when value invalid",
			args: args{
				query: "select * from test_keyspace.test_table where column1=",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "test with IN operator raw query",
			args: args{
				query: `select column1, column2 from test_keyspace.test_table where column1 = 'test' and column1 in ('abc', 'xyz');`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				TranslatedQuery: "SELECT column1,cf1['column2'] FROM test_table WHERE column1 = @value1 AND column1 IN UNNEST(@value2);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": "test",
					"value2": []string{"abc", "xyz"},
				},
				ParamKeys: []string{"value1", "value2"},
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:       "column1",
						Operator:     "IN",
						Value:        "@value2",
						IsPrimaryKey: true,
					},
				},
			},
		},
		{
			name: "test with IN operator prepared query",
			args: args{
				query: `select column1, column2 from test_keyspace.test_table where column1 = '?' and column1 in ('?', '?');`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				TranslatedQuery: "SELECT column1,cf1['column2'] FROM test_table WHERE column1 = @value1 AND column1 IN UNNEST(@value2);",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{},
				ParamKeys:       []string{"value1", "value2"},
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:       "column1",
						Operator:     "IN",
						Value:        "@value2",
						IsPrimaryKey: true,
					},
				},
			},
		},
		{
			name: "Empty Query",
			args: args{
				query: "",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Query Without Select Object",
			args: args{
				query: "UPDATE table_name SET column1 = 'new_value1', column2 = 'new_value2' WHERE condition;",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "query with missing limit value",
			args: args{
				query: `select column1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT;`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query with LIMIT before ORDER BY",
			args: args{
				query: `select column1, column2, column3 from test_keyspace.test_table LIMIT 100 ORDER BY column1;`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query with non-existent column in ORDER BY",
			args: args{
				query: `select column1, column2, column3 from test_keyspace.test_table ORDER BY column12343 LIMIT 100;`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query with negative LIMIT value",
			args: args{
				query: `select column1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT -100;`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query with duplicate negative LIMIT value (potential duplicate test case)",
			args: args{
				query: `select column1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT -100;`,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaMapping := &schemaMapping.SchemaMappingConfig{
				Logger:             tt.fields.Logger,
				TablesMetaData:     mockSchemaMappingConfig,
				PkMetadataCache:    mockPkMetadata,
				SystemColumnFamily: "cf1",
			}
			tr := &Translator{
				Logger:              tt.fields.Logger,
				SchemaMappingConfig: schemaMapping,
			}
			got, err := tr.TranslateSelectQuerytoBigtable(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.TranslateSelectQuerytoBigtable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				assert.Equal(t, tt.want.TranslatedQuery, got.TranslatedQuery)
				assert.Equal(t, tt.want.Params, got.Params)
				assert.Equal(t, tt.want.ParamKeys, got.ParamKeys)
				assert.Equal(t, tt.want.Clauses, got.Clauses)
				assert.Equal(t, tt.want.Keyspace, got.Keyspace)
			}
		})
	}
}

func Test_getBigtableSelectQuery(t *testing.T) {
	schemaMap := &schemaMapping.SchemaMappingConfig{
		Logger:             zap.NewNop(),
		TablesMetaData:     mockSchemaMappingConfig,
		SystemColumnFamily: "cf1",
	}
	tr := &Translator{
		Logger:              zap.NewNop(),
		SchemaMappingConfig: schemaMap,
	}

	type args struct {
		data *SelectQueryMap
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "It should faile because order by not support map data type",
			args: args{
				data: &SelectQueryMap{
					QueryType: "select",
					Table:     "test_table",
					Keyspace:  "test_keyspace",
					ColumnMeta: ColumnMeta{
						Star: true,
					},
					Limit: Limit{
						IsLimit: true,
						Count:   "1000",
					},
					OrderBy: OrderBy{
						IsOrderBy: true,
						Column:    "map_text_text",
						Operation: "asc",
					},
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "It should pass with @limit value",
			args: args{
				data: &SelectQueryMap{
					QueryType: "select",
					Table:     "test_table",
					Keyspace:  "test_keyspace",
					ColumnMeta: ColumnMeta{
						Star: true,
					},
					Limit: Limit{
						IsLimit: true,
						Count:   "?",
					},
				},
			},
			want:    "SELECT * FROM test_table LIMIT @limitValue;",
			wantErr: false,
		},
		{
			name: "success",
			args: args{
				data: &SelectQueryMap{
					QueryType: "select",
					Table:     "test_table",
					ColumnMeta: ColumnMeta{
						Star: true,
					},
					Limit: Limit{
						IsLimit: true,
						Count:   "1000",
					},
					OrderBy: OrderBy{
						IsOrderBy: false,
						Column:    "",
						Operation: "",
					},
				},
			},
			want:    "SELECT * FROM test_table LIMIT 1000;",
			wantErr: false,
		},
		{
			name: "GROUP BY with multiple columns",
			args: args{
				data: &SelectQueryMap{
					QueryType: "select",
					Table:     "test_table",
					Keyspace:  "test_keyspace",
					ColumnMeta: ColumnMeta{
						Star: false,
						Column: []schemaMapping.SelectedColumns{
							{Name: "column1"},
							{Name: "column6"},
						},
					},
					GroupByColumns: []string{"column1", "column6"},
				},
			},
			want:    "SELECT column1,TO_INT64(cf1['column6']) FROM test_table GROUP BY column1,TO_INT64(cf1['column6']);",
			wantErr: false,
		},
		{
			name: "GROUP BY with unsupported collection type",
			args: args{
				data: &SelectQueryMap{
					QueryType: "select",
					Table:     "test_table",
					Keyspace:  "test_keyspace",
					ColumnMeta: ColumnMeta{
						Star: false,
						Column: []schemaMapping.SelectedColumns{
							{Name: "column1"},
							{Name: "map_text_text"},
						},
					},
					GroupByColumns: []string{"column1", "map_text_text"},
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "error",
			args: args{
				data: &SelectQueryMap{},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getBigtableSelectQuery(tr, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("getBigtableSelectQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getBigtableSelectQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestInferDataType(t *testing.T) {
	tests := []struct {
		methodName   string
		expectedType string
		expectedErr  error
	}{
		{"count", "bigint", nil},
		{"round", "float", nil},
		{"unknown", "", errors.New("unknown function 'unknown'")},
	}

	for _, test := range tests {
		t.Run(test.methodName, func(t *testing.T) {
			result, err := inferDataType(test.methodName)
			if result != test.expectedType {
				t.Errorf("Expected type %s, got %s", test.expectedType, result)
			}
			if (err != nil && test.expectedErr == nil) || (err == nil && test.expectedErr != nil) {
				t.Errorf("Expected error %v, got %v", test.expectedErr, err)
			}

			if err != nil && test.expectedErr != nil && err.Error() != test.expectedErr.Error() {
				t.Errorf("Expected error message %v, got %v", test.expectedErr.Error(), err.Error())
			}
		})
	}
}

func Test_parseTableFromSelect(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    *TableObj
		wantErr bool
	}{
		{
			name:  "Valid keyspace.table format",
			query: "SELECT * FROM test_keyspace.test_table",
			want: &TableObj{
				TableName:    "test_table",
				KeyspaceName: "test_keyspace",
			},
			wantErr: false,
		},
		{
			name:    "Missing keyspace",
			query:   "SELECT * FROM test_table",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Empty query",
			query:   "",
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewCqlParser(tt.query, false)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			var fromSpec cql.IFromSpecContext
			if selectStmt := p.Select_(); selectStmt != nil {
				fromSpec = selectStmt.FromSpec()
			}

			got, err := parseTableFromSelect(fromSpec)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTableFromSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseTableFromSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseOrderByFromSelect(t *testing.T) {
	type args struct {
		input cql.IOrderSpecContext
	}
	tests := []struct {
		name    string
		args    args
		want    OrderBy
		wantErr bool
	}{
		{
			name: "Empty Input",
			args: args{
				input: nil,
			},
			want: OrderBy{
				IsOrderBy: false,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOrderByFromSelect(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseOrderByFromSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseOrderByFromSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_processFunctionColumn(t *testing.T) {
	// Create a mock schema mapping config
	mockSchemaMappingConfig := map[string]map[string]map[string]*schemaMapping.Column{
		"test_keyspace": {
			"user_info": {
				"age": {
					ColumnName: "age",
					CQLType:    "bigint",
					ColumnType: "bigint",
				},
				"balance": {
					ColumnName: "balance",
					CQLType:    "float",
					ColumnType: "float",
				},
				"name": {
					ColumnName: "name",
					CQLType:    "text",
					ColumnType: "string",
				},
				"code": {
					ColumnName: "code",
					CQLType:    "int",
					ColumnType: "int",
				},
			},
		},
	}

	tests := []struct {
		name           string
		columnMetadata schemaMapping.SelectedColumns
		tableName      string
		keySpace       string
		inputColumns   []string
		wantColumns    []string
		wantDataType   string
		wantErr        bool
		errMsg         string
	}{
		{
			name: "COUNT(*)",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "count",
				FuncColumnName: "*",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantColumns: []string{
				"count(*)",
			},
			wantDataType: "bigint",
			wantErr:      false,
		},
		{
			name: "AVG with numeric column",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "avg",
				FuncColumnName: "age",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantColumns: []string{
				"avg(TO_INT64(cf1['age']))",
			},
			wantDataType: "bigint",
			wantErr:      false,
		},
		{
			name: "SUM with alias",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "sum",
				FuncColumnName: "balance",
				IsFunc:         true,
				IsAs:           true,
				Alias:          "total_balance",
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantColumns: []string{
				"sum(TO_FLOAT32(cf1['balance'])) as total_balance",
			},
			wantDataType: "float",
			wantErr:      false,
		},
		{
			name: "Invalid function",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "invalid_func",
				FuncColumnName: "age",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantErr:      true,
			errMsg:       "unknown function 'invalid_func'",
		},
		{
			name: "Non-numeric column in aggregate",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "sum",
				FuncColumnName: "name",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantErr:      true,
			errMsg:       "column not supported for aggregate",
		},
		{
			name: "AVG with float column",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "avg",
				FuncColumnName: "balance",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantColumns: []string{
				"avg(TO_FLOAT32(cf1['balance']))",
			},
			wantDataType: "float",
			wantErr:      false,
		},
		{
			name: "MIN with int column",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "min",
				FuncColumnName: "code",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantColumns: []string{
				"min(TO_INT64(cf1['code']))",
			},
			wantDataType: "int",
			wantErr:      false,
		},
		{
			name: "MAX with alias",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "max",
				FuncColumnName: "age",
				IsFunc:         true,
				IsAs:           true,
				Alias:          "max_age",
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantColumns: []string{
				"max(TO_INT64(cf1['age'])) as max_age",
			},
			wantDataType: "bigint",
			wantErr:      false,
		},
		{
			name: "Missing column metadata",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "avg",
				FuncColumnName: "nonexistent",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantErr:      true,
			errMsg:       "column metadata not found for column 'nonexistent' in table 'user_info' and keyspace 'test_keyspace'",
		},
		{
			name: "Empty function name",
			columnMetadata: schemaMapping.SelectedColumns{
				FuncName:       "",
				FuncColumnName: "age",
				IsFunc:         true,
			},
			tableName:    "user_info",
			keySpace:     "test_keyspace",
			inputColumns: []string{},
			wantErr:      true,
			errMsg:       "unknown function ''",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := &Translator{
				SchemaMappingConfig: &schemaMapping.SchemaMappingConfig{
					TablesMetaData:     mockSchemaMappingConfig,
					SystemColumnFamily: "cf1",
				},
			}

			gotColumns, gotDataType, err := processFunctionColumn(translator, tt.columnMetadata, tt.tableName, tt.keySpace, tt.inputColumns)

			if tt.wantErr {
				if err == nil {
					t.Errorf("processFunctionColumn() error = nil, wantErr %v", tt.wantErr)
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("processFunctionColumn() error = %v, wantErr %v", err, tt.errMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("processFunctionColumn() unexpected error = %v", err)
				return
			}

			if !reflect.DeepEqual(gotColumns, tt.wantColumns) {
				t.Errorf("processFunctionColumn() gotColumns = %v, want %v", gotColumns, tt.wantColumns)
			}

			if gotDataType != tt.wantDataType {
				t.Errorf("processFunctionColumn() gotDataType = %v, want %v", gotDataType, tt.wantDataType)
			}
		})
	}
}

func Test_parseColumnsFromSelectWithParser(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    ColumnMeta
		wantErr bool
	}{
		{
			name:    "star query",
			query:   "SELECT * FROM test_table",
			want:    ColumnMeta{Star: true},
			wantErr: false,
		},
		{
			name:  "single column",
			query: "SELECT pk_1_text FROM test_table",
			want: ColumnMeta{
				Column: []schemaMapping.SelectedColumns{
					{Name: "pk_1_text"},
				},
			},
			wantErr: false,
		},
		{
			name:  "multiple columns",
			query: "SELECT pk_1_text, blob_col, bool_col FROM test_table",
			want: ColumnMeta{
				Column: []schemaMapping.SelectedColumns{
					{Name: "pk_1_text"},
					{Name: "blob_col"},
					{Name: "bool_col"},
				},
			},
			wantErr: false,
		},
		{
			name:  "column with alias",
			query: "SELECT pk_1_text AS alias1 FROM test_table",
			want: ColumnMeta{
				Column: []schemaMapping.SelectedColumns{
					{Name: "pk_1_text", IsAs: true, Alias: "alias1"},
				},
			},
			wantErr: false,
		},
		{
			name:  "function with star",
			query: "SELECT COUNT(*) FROM test_table",
			want: ColumnMeta{
				Column: []schemaMapping.SelectedColumns{
					{Name: "count_*", IsFunc: true, FuncName: "count", Alias: "", FuncColumnName: "*"},
				},
			},
			wantErr: false,
		},
		{
			name:  "writetime function",
			query: "SELECT name,WRITETIME(pk_1_text) AS wt FROM test_table",
			want: ColumnMeta{
				Column: []schemaMapping.SelectedColumns{
					{
						Name: "name",
					},
					{
						Name:              "writetime(pk_1_text)",
						Alias:             "wt",
						IsAs:              true,
						FuncColumnName:    "pk_1_text",
						IsWriteTimeColumn: true,
					},
				},
			},
			wantErr: false,
		},
		{
			name:  "map access",
			query: "SELECT map_col['key1'] FROM test_table",
			want: ColumnMeta{
				Column: []schemaMapping.SelectedColumns{
					{Name: "map_col_key1", MapKey: "key1", Alias: "", MapColumnName: "map_col"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewCqlParser(tt.query, false)
			if err != nil {
				t.Fatalf("Failed to create parser: %v", err)
			}

			selectElements := p.Select_().SelectElements()

			got, err := parseColumnsFromSelect(selectElements)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseColumnsFromSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseColumnsFromSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseGroupByColumn(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "Single column",
			query:    "select * from table group by col1",
			expected: []string{"col1"},
		},
		{
			name:     "Multiple columns",
			query:    "select * from table group by col1, col2, col3",
			expected: []string{"col1", "col2", "col3"},
		},
		{
			name:     "With semicolon",
			query:    "select * from table group by col1;",
			expected: []string{"col1"},
		},
		{
			name:     "With ORDER BY",
			query:    "select * from table group by col1 order by col2",
			expected: []string{"col1"},
		},
		{
			name:     "With LIMIT",
			query:    "select * from table group by col1 limit 10",
			expected: []string{"col1"},
		},
		{
			name:     "No GROUP BY",
			query:    "select * from table",
			expected: nil,
		},
		{
			name:     "Malformed GROUP BY",
			query:    "select * from table group by",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseGroupByColumn(tt.query)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("parseGroupByColumn() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func Test_dtAllowedInAggregate(t *testing.T) {
	tests := []struct {
		dataType string
		expected bool
	}{
		{"int", true},
		{"bigint", true},
		{"float", true},
		{"double", true},
		{"text", false},
		{"boolean", false},
		{"timestamp", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := dtAllowedInAggregate(tt.dataType)
			if got != tt.expected {
				t.Errorf("dtAllowedInAggregate(%q) = %v, want %v", tt.dataType, got, tt.expected)
			}
		})
	}
}

func Test_parseLimitFromSelect(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    Limit
		wantErr bool
		errMsg  string
	}{
		{
			name:  "Valid numeric limit",
			query: "SELECT * FROM test_table LIMIT 10",
			want:  Limit{IsLimit: true, Count: "10"},
		},
		{
			name:  "Placeholder limit",
			query: "SELECT * FROM test_table LIMIT ?",
			want:  Limit{IsLimit: true, Count: "?"},
		},
		{
			name:  "No limit",
			query: "SELECT * FROM test_table",
			want:  Limit{IsLimit: false},
		},
		{
			name:    "Invalid limit (negative)",
			query:   "SELECT * FROM test_table LIMIT -10",
			want:    Limit{},
			wantErr: true,
			errMsg:  "no viable alternative at input '-10'",
		},
		{
			name:    "Invalid limit (zero)",
			query:   "SELECT * FROM test_table LIMIT 0",
			want:    Limit{},
			wantErr: true,
			errMsg:  "no viable alternative at input '0'",
		},
		{
			name:    "Invalid limit (non-numeric)",
			query:   "SELECT * FROM test_table LIMIT abc",
			want:    Limit{},
			wantErr: true,
			errMsg:  "no viable alternative at input 'abc'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewCqlParser(tt.query, false)
			if err != nil {
				t.Fatalf("Failed to create parser: %v", err)
			}

			var limitSpec cql.ILimitSpecContext
			if selectStmt := p.Select_(); selectStmt != nil {
				limitSpec = selectStmt.LimitSpec()
			}

			got, err := parseLimitFromSelect(limitSpec)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseLimitFromSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("parseLimitFromSelect() error = %v, wantErr containing %q", err, tt.errMsg)
				}
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseLimitFromSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_funcAllowedInAggregate(t *testing.T) {
	tests := []struct {
		funcName string
		expected bool
	}{
		{"avg", true},
		{"sum", true},
		{"min", true},
		{"max", true},
		{"count", true},
		{"AVG", true}, // case insensitive
		{"Sum", true}, // case insensitive
		{"unknown", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.funcName, func(t *testing.T) {
			got := funcAllowedInAggregate(tt.funcName)
			if got != tt.expected {
				t.Errorf("funcAllowedInAggregate(%q) = %v, want %v", tt.funcName, got, tt.expected)
			}
		})
	}
}

func TestProcessStrings(t *testing.T) {
	// Setup mock schema mapping config
	mockSchemaMappingConfig := map[string]map[string]map[string]*schemaMapping.Column{
		"test_keyspace": {
			"user_info": {
				"name": &schemaMapping.Column{
					ColumnName: "name",
					CQLType:    "text",
					ColumnType: "text",
				},
				"age": &schemaMapping.Column{
					ColumnName: "age",
					CQLType:    "bigint",
					ColumnType: "bigint",
				},
				"code": &schemaMapping.Column{
					ColumnName: "code",
					CQLType:    "int",
					ColumnType: "int",
				},
				"map_col": &schemaMapping.Column{
					ColumnName: "map_col",
					CQLType:    "map<text,text>",
					ColumnType: "map<text,text>",
				},
			},
		},
	}

	tests := []struct {
		name            string
		selectedColumns []schemaMapping.SelectedColumns
		tableName       string
		keySpace        string
		isGroupBy       bool
		wantAliases     map[string]AsKeywordMeta
		wantColumns     []string
		wantErr         bool
	}{
		{
			name: "Simple column selection",
			selectedColumns: []schemaMapping.SelectedColumns{
				{Name: "name"},
				{Name: "age"},
			},
			tableName:   "user_info",
			keySpace:    "test_keyspace",
			isGroupBy:   false,
			wantAliases: map[string]AsKeywordMeta{},
			wantColumns: []string{
				"cf1['name']",
				"cf1['age']",
			},
			wantErr: false,
		},
		{
			name: "Column with alias",
			selectedColumns: []schemaMapping.SelectedColumns{
				{Name: "name", IsAs: true, Alias: "username"},
				{Name: "age"},
			},
			tableName: "user_info",
			keySpace:  "test_keyspace",
			isGroupBy: false,
			wantAliases: map[string]AsKeywordMeta{
				"username": {
					Name:     "name",
					Alias:    "username",
					IsFunc:   false,
					DataType: "text",
				},
			},
			wantColumns: []string{
				"cf1['name'] as username",
				"cf1['age']",
			},
			wantErr: false,
		},
		{
			name: "Aggregate function",
			selectedColumns: []schemaMapping.SelectedColumns{
				{
					Name:           "count_age",
					IsFunc:         true,
					FuncName:       "count",
					FuncColumnName: "age",
					IsAs:           true,
					Alias:          "age_count",
				},
			},
			tableName: "user_info",
			keySpace:  "test_keyspace",
			isGroupBy: false,
			wantAliases: map[string]AsKeywordMeta{
				"age_count": {
					Name:     "age",
					IsFunc:   true,
					DataType: "bigint",
					Alias:    "age_count",
				},
			},
			wantColumns: []string{"count(TO_INT64(cf1['age'])) as age_count"},
			wantErr:     false,
		},
		{
			name: "Invalid column",
			selectedColumns: []schemaMapping.SelectedColumns{
				{Name: "invalid_column"},
			},
			tableName: "user_info",
			keySpace:  "test_keyspace",
			isGroupBy: false,
			wantErr:   true,
		},
		{
			name: "Regular column with alias",
			selectedColumns: []schemaMapping.SelectedColumns{
				{Name: "age", IsAs: true, Alias: "user_age"},
			},
			tableName: "user_info",
			keySpace:  "test_keyspace",
			isGroupBy: false,
			wantAliases: map[string]AsKeywordMeta{
				"user_age": {
					Name:     "age",
					Alias:    "user_age",
					DataType: "bigint",
				},
			},
			wantColumns: []string{"cf1['age'] as user_age"},
			wantErr:     false,
		},
		{
			name: "Collection column with alias",
			selectedColumns: []schemaMapping.SelectedColumns{
				{Name: "map_col", IsAs: true, Alias: "renamed_map"},
			},
			tableName: "user_info",
			keySpace:  "test_keyspace",
			isGroupBy: false,
			wantAliases: map[string]AsKeywordMeta{
				"renamed_map": {
					Name:     "map_col",
					Alias:    "renamed_map",
					DataType: "map<text,text>",
				},
			},
			wantColumns: []string{"cf1['map_col'] as renamed_map"},
			wantErr:     false,
		},
		{
			name: "Column with alias in GROUP BY",
			selectedColumns: []schemaMapping.SelectedColumns{
				{Name: "age", IsAs: true, Alias: "user_age"},
			},
			tableName: "user_info",
			keySpace:  "test_keyspace",
			isGroupBy: true,
			wantAliases: map[string]AsKeywordMeta{
				"user_age": {
					Name:     "age",
					Alias:    "user_age",
					DataType: "bigint",
				},
			},
			wantColumns: []string{"TO_INT64(cf1['age']) as user_age"},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Translator{
				Logger: zap.NewNop(),
				SchemaMappingConfig: &schemaMapping.SchemaMappingConfig{
					TablesMetaData:     mockSchemaMappingConfig,
					SystemColumnFamily: "cf1",
				},
			}

			gotAliases, gotColumns, err := processStrings(tr, tt.selectedColumns, tt.tableName, tt.keySpace, tt.isGroupBy)

			if (err != nil) != tt.wantErr {
				t.Errorf("processStrings() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if !reflect.DeepEqual(gotAliases, tt.wantAliases) {
					t.Errorf("processStrings() gotAliases = %v, want %v", gotAliases, tt.wantAliases)
				}
				if !reflect.DeepEqual(gotColumns, tt.wantColumns) {
					t.Errorf("processStrings() gotColumns = %v, want %v", gotColumns, tt.wantColumns)
				}
			}
		})
	}
}
func Test_processAsColumn(t *testing.T) {
	tests := []struct {
		name           string
		columnMetadata schemaMapping.SelectedColumns
		tableName      string
		columnFamily   string
		colMeta        *schemaMapping.Column
		columns        []string
		isGroupBy      bool
		want           []string
	}{
		{
			name: "Non-collection column with GROUP BY",
			columnMetadata: schemaMapping.SelectedColumns{
				Name:  "pk_1_text",
				Alias: "col1",
			},
			tableName:    "test_table",
			columnFamily: "cf1",
			colMeta: &schemaMapping.Column{
				IsCollection: false,
				ColumnType:   "text",
				ColumnName:   "pk_1_text",
			},
			columns:   []string{},
			isGroupBy: true,
			want:      []string{"cf1['pk_1_text'] as col1"},
		},
		{
			name: "Non-collection column without GROUP BY",
			columnMetadata: schemaMapping.SelectedColumns{
				Name:  "pk_1_text",
				Alias: "col1",
			},
			tableName:    "test_table",
			columnFamily: "cf1",
			colMeta: &schemaMapping.Column{
				ColumnName:   "pk_1_text",
				IsCollection: false,
			},
			columns:   []string{},
			isGroupBy: false,
			want:      []string{"cf1['pk_1_text'] as col1"},
		},
		{
			name: "Collection column without GROUP BY",
			columnMetadata: schemaMapping.SelectedColumns{
				Name:  "map_column",
				Alias: "map1",
			},
			tableName:    "test_table",
			columnFamily: "cf1",
			colMeta: &schemaMapping.Column{
				IsCollection: true,
			},
			columns:   []string{},
			isGroupBy: false,
			want:      []string{"`map_column` as map1"},
		},
		{
			name: "With existing columns",
			columnMetadata: schemaMapping.SelectedColumns{
				Name:  "pk_1_text",
				Alias: "col1",
			},
			tableName:    "test_table",
			columnFamily: "cf1",
			colMeta: &schemaMapping.Column{
				IsCollection: false,
			},
			columns:   []string{"existing_column"},
			isGroupBy: false,
			want:      []string{"existing_column", "cf1['pk_1_text'] as col1"},
		},
		{
			name: "WriteTime column",
			columnMetadata: schemaMapping.SelectedColumns{
				Name:              "test_table",
				Alias:             "wt",
				IsWriteTimeColumn: true,
			},
			tableName:    "test_table",
			columnFamily: "cf1",
			colMeta: &schemaMapping.Column{
				IsCollection: false,
			},
			columns:   []string{},
			isGroupBy: false,
			want:      []string{"cf1['test_table'] as wt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := processAsColumn(tt.columnMetadata, tt.tableName, tt.columnFamily, tt.colMeta, tt.columns, tt.isGroupBy)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("processAsColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}
