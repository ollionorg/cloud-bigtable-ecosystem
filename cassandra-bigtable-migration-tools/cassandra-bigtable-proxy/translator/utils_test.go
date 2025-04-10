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
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/third_party/datastax/proxycore"
	"go.uber.org/zap"
)

func Test_convertMapString(t *testing.T) {
	type args struct {
		dataStr string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "sucess",
			args: args{
				dataStr: "{i3=true, 5023.0=true}",
			},
			want: map[string]interface{}{
				"i3":     true,
				"5023.0": true,
			},
			wantErr: false,
		},
		{
			name: "error",
			args: args{
				dataStr: "test",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertMapString(tt.args.dataStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertMapString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertMapString() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Helper function to create a mock TerminalNode
func createMockTerminalNode(token antlr.Token) antlr.Tree {
	return antlr.NewTerminalNodeImpl(token)
}

func Test_treetoString(t *testing.T) {
	tests := []struct {
		name   string
		input  antlr.Tree
		expect string
	}{
		{
			name:   "TerminalNode",
			input:  createMockTerminalNode(antlr.NewCommonToken(&antlr.TokenSourceCharStreamPair{}, 1, 1, 1, 1)),
			expect: "", // Modify the expected output accordingly
		},
		{
			name:   "NonTerminalNode",
			input:  antlr.NewErrorNodeImpl(nil),
			expect: "", // Modify the expected output accordingly
		},
		{
			name:   "NilInput",
			input:  nil,
			expect: "", // Modify the expected output accordingly
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := treetoString(tt.input)

			if result != tt.expect {
				t.Errorf("Test '%s' failed: expected '%s', got '%s'", tt.name, tt.expect, result)
			}
		})
	}
}

func Test_hasWhere(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "success",
			args: args{
				query: "select * from table where id = 1;",
			},
			want: true,
		},
		{
			name: "no where clause",
			args: args{
				query: "select * from table;",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasWhere(tt.args.query); got != tt.want {
				t.Errorf("hasWhere() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestParseTimestamp tests the parseTimestamp function with various timestamp formats.
func TestParseTimestamp(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "ISO 8601 format",
			input:    "2024-02-05T14:00:00Z",
			expected: time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
		},
		{
			name:     "Common date-time format",
			input:    "2024-02-05 14:00:00",
			expected: time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
		},
		{
			name:     "Unix timestamp (seconds)",
			input:    "1672522562",
			expected: time.Unix(1672522562, 0),
		},
		{
			name:     "Unix timestamp (milliseconds)",
			input:    "1672522562000",
			expected: time.Unix(0, 1672522562000*int64(time.Millisecond)),
		},
		{
			name:     "Unix timestamp (microseconds)",
			input:    "1672522562000000",
			expected: time.Unix(0, 1672522562000000*int64(time.Microsecond)),
		},
		{
			name:    "Invalid format",
			input:   "invalid-timestamp",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTimestamp(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseTimestamp() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			// Allow a small margin of error for floating-point timestamp comparisons
			if !tc.wantErr {
				delta := got.Sub(tc.expected)
				if delta > time.Millisecond || delta < -time.Millisecond {
					t.Errorf("parseTimestamp() = %v, want %v (delta: %v)", got, tc.expected, delta)
				}
			}
		})
	}
}

// TestConvertMap tests the convertMap function with various inputs.
func TestConvertToMap(t *testing.T) {
	cases := []struct {
		name     string
		dataStr  string
		cqlType  string
		expected map[string]interface{}
		wantErr  bool
	}{
		{
			name:    "Valid map<varchar, timestamp>",
			dataStr: `{"key1":"2024-02-05T14:00:00Z", "key2":"2024-02-05T14:00:00Z"}`,
			cqlType: "map<varchar, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
				"key2": time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<varchar, timestamp> Case 2",
			dataStr: `{"key1":"2023-01-30T09:00:00Z"}`,
			cqlType: "map<varchar, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 9, 0, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<varchar, timestamp> time in unix seconds",
			dataStr: `{"key1":1675075200}`,
			cqlType: "map<varchar, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 10, 40, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<varchar, timestamp> time in unix miliseconds",
			dataStr: `{"key1":1675075200}`,
			cqlType: "map<varchar, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 10, 40, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<varchar, timestamp> time in unix miliseconds and equal operator",
			dataStr: `{"key1"=1675075200}`,
			cqlType: "map<varchar, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 10, 40, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<varchar, varchar> with equal ",
			dataStr: `{"key1"="1675075200"}`,
			cqlType: "map<varchar, varchar>",
			expected: map[string]interface{}{
				"key1": "1675075200",
			},
		},
		{
			name:    "Valid map<varchar, varchar> with colon",
			dataStr: `{"key1":"1675075200"}`,
			cqlType: "map<varchar, varchar>",
			expected: map[string]interface{}{
				"key1": "1675075200",
			},
		},
		{
			name:    "Invalid pair formate",
			dataStr: `{"key11675075200, "sdfdgdgdf"}`,
			cqlType: "map<varchar, varchar>",
			wantErr: true,
		},
		{
			name:    "Invalid timestamp format",
			dataStr: `{"key1": "invalid-timestamp"}`,
			cqlType: "map<varchar, timestamp>",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertToMap(tc.dataStr, tc.cqlType)
			if (err != nil) != tc.wantErr {
				t.Errorf("convertMap() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tc.expected) && !tc.wantErr {
				t.Errorf("convertMap() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestMapOfStringToString(t *testing.T) {
	cases := []struct {
		name     string
		pairs    []string
		expected map[string]interface{}
		wantErr  bool
	}{
		{
			name:  "Valid pairs with colon separator",
			pairs: []string{"key1:value1", "key2:value2"},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			wantErr: false,
		},
		{
			name:  "Valid pairs with equal separator",
			pairs: []string{"key1=value1", "key2=value2"},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			wantErr: false,
		},
		{
			name:    "Invalid pair format",
			pairs:   []string{"key1-value1"},
			wantErr: true,
		},
		{
			name:  "Mixed separators",
			pairs: []string{"key1:value1", "key2=value2"},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mapOfStringToString(tc.pairs)
			if (err != nil) != tc.wantErr {
				t.Errorf("mapOfStringToString() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tc.expected) && !tc.wantErr {
				t.Errorf("mapOfStringToString() got = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestPrimitivesToString(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
		err      bool
	}{
		{"hello", "hello", false},
		{int32(123), "123", false},
		{int(456), "456", false},
		{int64(789), "789", false},
		{float32(1.23), "1.23", false},
		{float64(4.56), "4.56", false},
		{true, "true", false},
		{false, "false", false},
		{complex(1, 1), "", true}, // unsupported type
	}

	for _, test := range tests {
		output, err := primitivesToString(test.input)
		if (err != nil) != test.err {
			t.Errorf("primitivesToString(%v) unexpected error status: %v", test.input, err)
			continue
		}
		if output != test.expected {
			t.Errorf("primitivesToString(%v) = %v; want %v", test.input, output, test.expected)
		}
	}
}

func TestTranslator_GetAllColumns(t *testing.T) {
	type fields struct {
		Logger              *zap.Logger
		SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	}
	type args struct {
		tableName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		want1   string
		wantErr bool
	}{
		{
			name: "Valid Input",
			fields: fields{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			args: args{
				tableName: "test_table",
			},
			want:    []string{"bigint_col", "blob_col", "bool_col", "column1", "column10", "column2", "column3", "column5", "column6", "column9", "int_col", "timestamp_col"},
			want1:   "cf1",
			wantErr: false,
		},
		{
			name: "Valid Input",
			fields: fields{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			args: args{
				tableName: "test_table123",
			},
			want:    nil,
			want1:   "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Translator{
				Logger:              tt.fields.Logger,
				SchemaMappingConfig: tt.fields.SchemaMappingConfig,
			}
			got, got1, err := tr.GetAllColumns(tt.args.tableName, "test_keyspace")
			sort.Strings(got)
			sort.Strings(tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.GetAllColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Translator.GetAllColumns() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Translator.GetAllColumns() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestExtractCFAndQualifiersforMap(t *testing.T) {
	tests := []struct {
		input         string
		expectedCF    string
		expectedQuals []string
		expectedErr   bool
	}{
		{"cf-{q1,q2,q3}", "cf", []string{"q1", "q2", "q3"}, false},
		{"cf-{q1}", "cf", []string{"q1"}, false},
		{"cf-{}", "cf", []string{""}, false},
		{"cf", "", nil, true},
		{"-{}", "", nil, true},
		{"cf-{,}", "cf", []string{"", ""}, false},
		{"cf-{q1, q2 , q3}", "cf", []string{"q1", " q2 ", " q3"}, false},
		{"", "", nil, true},
	}

	for _, tt := range tests {
		cf, quals, err := ExtractCFAndQualifiersforMap(tt.input)
		if (err != nil) != tt.expectedErr {
			t.Errorf("ExtractCFAndQualifiersforMap(%v) error = %v, expectedErr %v", tt.input, err, tt.expectedErr)
		}
		if cf != tt.expectedCF {
			t.Errorf("ExtractCFAndQualifiersforMap(%v) cf = %v, expected %v", tt.input, cf, tt.expectedCF)
		}
		if !reflect.DeepEqual(quals, tt.expectedQuals) {
			t.Errorf("ExtractCFAndQualifiersforMap(%v) quals = %v, expected %v", tt.input, quals, tt.expectedQuals)
		}
	}
}

func Test_processCollectionColumnsForRawQueries(t *testing.T) {
	trueVal, _ := formatValues("true", "boolean", primitive.ProtocolVersion4)
	falseVal, _ := formatValues("false", "boolean", primitive.ProtocolVersion4)
	textVal, _ := formatValues("test", "varchar", primitive.ProtocolVersion4)
	textVal2, _ := formatValues("test2", "varchar", primitive.ProtocolVersion4)
	emptyVal, _ := formatValues("", "varchar", primitive.ProtocolVersion4)
	intVal, _ := formatValues("10", "int", primitive.ProtocolVersion4)
	intVal2, _ := formatValues("20", "int", primitive.ProtocolVersion4)
	floatVal, _ := formatValues("10.10", "float", primitive.ProtocolVersion4)
	doubleVal, _ := formatValues("10.10101", "double", primitive.ProtocolVersion4)
	timestampVal, _ := formatValues("1234567890", "timestamp", primitive.ProtocolVersion4)
	bigintVal, _ := formatValues("1234567890", "bigint", primitive.ProtocolVersion4)
	bigintVal2, _ := formatValues("1234567891", "bigint", primitive.ProtocolVersion4)
	type args struct {
		columns   []Column
		values    []interface{}
		tableName string
		t         *Translator
	}
	tests := []struct {
		name    string
		args    args
		want    []Column
		want1   []interface{}
		want2   []string
		want3   []Column
		wantErr bool
	}{
		{
			name: "Valid Input For Text Boolean",
			args: args{
				columns:   []Column{{Name: "map_text_bool_col", ColumnFamily: "map_text_bool_col", CQLType: "map<varchar,boolean>"}}, // Updated column name
				values:    []interface{}{"{test:true}"},
				tableName: "test_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_bool_col", CQLType: "bigint"}, // Updated column family
			},
			want1: []interface{}{trueVal},
			want2: []string{
				"map_text_bool_col", // Updated column name
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Text",
			args: args{
				columns:   []Column{{Name: "map_text_text", ColumnFamily: "map_text_text", CQLType: "map<varchar,varchar>"}},
				values:    []interface{}{"{test:test}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_text", CQLType: "varchar"},
			},
			want1: []interface{}{textVal},
			want2: []string{
				"map_text_text",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Int",
			args: args{
				columns:   []Column{{Name: "map_text_int", ColumnFamily: "map_text_int", CQLType: "map<varchar,int>"}},
				values:    []interface{}{"{test:10}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_int", CQLType: "bigint"},
			},
			want1: []interface{}{intVal},
			want2: []string{
				"map_text_int",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Float",
			args: args{
				columns:   []Column{{Name: "map_text_float", ColumnFamily: "map_text_float", CQLType: "map<varchar,float>"}},
				values:    []interface{}{"{test:10.10}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_float", CQLType: "float"},
			},
			want1: []interface{}{floatVal},
			want2: []string{
				"map_text_float",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Double",
			args: args{
				columns:   []Column{{Name: "map_text_double", ColumnFamily: "map_text_double", CQLType: "map<varchar,double>"}},
				values:    []interface{}{"{test:10.10101}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_double", CQLType: "double"},
			},
			want1: []interface{}{doubleVal},
			want2: []string{
				"map_text_double",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Timestamp",
			args: args{
				columns:   []Column{{Name: "map_text_timestamp", ColumnFamily: "map_text_timestamp", CQLType: "map<varchar,timestamp>"}},
				values:    []interface{}{"{test:1234567890}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_timestamp", CQLType: "timestamp"},
			},
			want1: []interface{}{timestampVal},
			want2: []string{
				"map_text_timestamp",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Text",
			args: args{
				columns:   []Column{{Name: "map_timestamp_text", ColumnFamily: "map_timestamp_text", CQLType: "map<timestamp,varchar>"}},
				values:    []interface{}{"{1234567890:test}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "map_timestamp_text", CQLType: "varchar"},
			},
			want1: []interface{}{textVal},
			want2: []string{
				"map_timestamp_text",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Int",
			args: args{
				columns:   []Column{{Name: "map_timestamp_int", ColumnFamily: "map_timestamp_int", CQLType: "map<timestamp,int>"}},
				values:    []interface{}{"{1234567890:10}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "map_timestamp_int", CQLType: "bigint"},
			},
			want1: []interface{}{intVal},
			want2: []string{
				"map_timestamp_int",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Boolean",
			args: args{
				columns:   []Column{{Name: "map_timestamp_boolean", ColumnFamily: "map_timestamp_boolean", CQLType: "map<timestamp,boolean>"}},
				values:    []interface{}{"{1234567890:true}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "map_timestamp_boolean", CQLType: "bigint"},
			},
			want1: []interface{}{trueVal},
			want2: []string{
				"map_timestamp_boolean",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Timestamp",
			args: args{
				columns:   []Column{{Name: "map_timestamp_timestamp", ColumnFamily: "map_timestamp_timestamp", CQLType: "map<timestamp,timestamp>"}},
				values:    []interface{}{"{1234567890:1234567890}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "map_timestamp_timestamp", CQLType: "timestamp"},
			},
			want1: []interface{}{timestampVal},
			want2: []string{
				"map_timestamp_timestamp",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Bigint",
			args: args{
				columns:   []Column{{Name: "map_timestamp_bigint", ColumnFamily: "map_timestamp_bigint", CQLType: "map<timestamp,bigint>"}},
				values:    []interface{}{"{1234567890:1234567890}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "map_timestamp_bigint", CQLType: "bigint"},
			},
			want1: []interface{}{bigintVal},
			want2: []string{
				"map_timestamp_bigint",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Float",
			args: args{
				columns:   []Column{{Name: "map_timestamp_float", ColumnFamily: "map_timestamp_float", CQLType: "map<timestamp,float>"}},
				values:    []interface{}{"{1234567890:10.10}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "map_timestamp_float", CQLType: "float"},
			},
			want1: []interface{}{floatVal},
			want2: []string{
				"map_timestamp_float",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Double",
			args: args{
				columns:   []Column{{Name: "map_timestamp_double", ColumnFamily: "map_timestamp_double", CQLType: "map<timestamp,double>"}},
				values:    []interface{}{"{1234567890:10.10101}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "map_timestamp_double", CQLType: "double"},
			},
			want1: []interface{}{doubleVal},
			want2: []string{
				"map_timestamp_double",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Set<Text>",
			args: args{
				columns:   []Column{{Name: "set_text", ColumnFamily: "set_text", CQLType: "set<varchar>"}},
				values:    []interface{}{"{'test'}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "test", ColumnFamily: "set_text", CQLType: "varchar"},
			},
			want1: []interface{}{emptyVal},
			want2: []string{
				"set_text",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Set<Boolean>",
			args: args{
				columns:   []Column{{Name: "set_boolean", ColumnFamily: "set_boolean", CQLType: "set<boolean>"}},
				values:    []interface{}{"{'true'}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1", ColumnFamily: "set_boolean", CQLType: "boolean"},
			},
			want1: []interface{}{emptyVal},
			want2: []string{
				"set_boolean",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Set<Int>",
			args: args{
				columns:   []Column{{Name: "set_int", ColumnFamily: "set_int", CQLType: "set<int>"}},
				values:    []interface{}{"{'10'}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "10", ColumnFamily: "set_int", CQLType: "int"},
			},
			want1: []interface{}{emptyVal},
			want2: []string{
				"set_int",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Set<Float>",
			args: args{
				columns:   []Column{{Name: "set_float", ColumnFamily: "set_float", CQLType: "set<float>"}},
				values:    []interface{}{"{10.10}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "10.10", ColumnFamily: "set_float", CQLType: "float"},
			},
			want1: []interface{}{emptyVal},
			want2: []string{
				"set_float",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Set<Double>",
			args: args{
				columns:   []Column{{Name: "set_double", ColumnFamily: "set_double", CQLType: "set<double>"}},
				values:    []interface{}{"{10.10101}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "10.10101", ColumnFamily: "set_double", CQLType: "double"},
			},
			want1: []interface{}{emptyVal},
			want2: []string{
				"set_double",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Set<BigInt>",
			args: args{
				columns:   []Column{{Name: "set_bigint", ColumnFamily: "set_bigint", CQLType: "set<bigint>"}},
				values:    []interface{}{"{1234567890123}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890123", ColumnFamily: "set_bigint", CQLType: "bigint"},
			},
			want1: []interface{}{emptyVal},
			want2: []string{
				"set_bigint",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For Set<Timestamp>",
			args: args{
				columns:   []Column{{Name: "set_timestamp", ColumnFamily: "set_timestamp", CQLType: "set<timestamp>"}},
				values:    []interface{}{"{'1234567890'}"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: "1234567890", ColumnFamily: "set_timestamp", CQLType: "timestamp"},
			},
			want1: []interface{}{emptyVal},
			want2: []string{
				"set_timestamp",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For List<Text>",
			args: args{
				columns:   []Column{{Name: "list_text", ColumnFamily: "list_text", CQLType: "list<varchar>"}},
				values:    []interface{}{"['test', 'test2']"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(0, 2, false)), ColumnFamily: "list_text", CQLType: "varchar"},
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(1, 2, false)), ColumnFamily: "list_text", CQLType: "varchar"},
			},
			want1: []interface{}{textVal, textVal2},
			want2: []string{
				"list_text",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For List<Int>",
			args: args{
				columns:   []Column{{Name: "list_int", ColumnFamily: "list_int", CQLType: "list<int>"}},
				values:    []interface{}{"[10, 20]"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(0, 2, false)), ColumnFamily: "list_int", CQLType: "int"},
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(1, 2, false)), ColumnFamily: "list_int", CQLType: "int"},
			},
			want1: []interface{}{intVal, intVal2},
			want2: []string{
				"list_int",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For List<BigInt>",
			args: args{
				columns:   []Column{{Name: "list_bigint", ColumnFamily: "list_bigint", CQLType: "list<bigint>"}},
				values:    []interface{}{"[1234567890, 1234567891]"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(0, 2, false)), ColumnFamily: "list_bigint", CQLType: "bigint"},
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(1, 2, false)), ColumnFamily: "list_bigint", CQLType: "bigint"},
			},
			want1: []interface{}{bigintVal, bigintVal2},
			want2: []string{
				"list_bigint",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For List<Boolean>",
			args: args{
				columns:   []Column{{Name: "list_boolean", ColumnFamily: "list_boolean", CQLType: "list<boolean>"}},
				values:    []interface{}{"[true, false]"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(0, 2, false)), ColumnFamily: "list_boolean", CQLType: "boolean"},
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(1, 2, false)), ColumnFamily: "list_boolean", CQLType: "boolean"},
			},
			want1: []interface{}{trueVal, falseVal},
			want2: []string{
				"list_boolean",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For List<Float>",
			args: args{
				columns:   []Column{{Name: "list_float", ColumnFamily: "list_float", CQLType: "list<float>"}},
				values:    []interface{}{"[10.10, 10.10]"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(0, 2, false)), ColumnFamily: "list_float", CQLType: "float"},
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(1, 2, false)), ColumnFamily: "list_float", CQLType: "float"},
			},
			want1: []interface{}{floatVal, floatVal},
			want2: []string{
				"list_float",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For List<Double>",
			args: args{
				columns:   []Column{{Name: "list_double", ColumnFamily: "list_double", CQLType: "list<double>"}},
				values:    []interface{}{"[10.10101, 10.10101]"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(0, 2, false)), ColumnFamily: "list_double", CQLType: "double"},
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(1, 2, false)), ColumnFamily: "list_double", CQLType: "double"},
			},
			want1: []interface{}{doubleVal, doubleVal},
			want2: []string{
				"list_double",
			},
			want3:   []Column{},
			wantErr: false,
		},
		{
			name: "Valid Input For List<Timestamp>",
			args: args{
				columns:   []Column{{Name: "list_timestamp", ColumnFamily: "list_timestamp", CQLType: "list<timestamp>"}},
				values:    []interface{}{"[1234567890, 1234567890]"},
				tableName: "non_primitive_table",
				t: &Translator{
					Logger:              zap.NewNop(),
					SchemaMappingConfig: GetSchemaMappingConfig(),
				},
			},
			want: []Column{
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(0, 2, false)), ColumnFamily: "list_timestamp", CQLType: "timestamp"},
				{Name: fmt.Sprintf("%x", getEncodedTimestamp(1, 2, false)), ColumnFamily: "list_timestamp", CQLType: "timestamp"},
			},
			want1: []interface{}{timestampVal, timestampVal},
			want2: []string{
				"list_timestamp",
			},
			want3:   []Column{},
			wantErr: false,
		},
	} // Correctly close the tests slice definition here

	for _, tt := range tests { // Start the loop after the slice definition
		t.Run(tt.name, func(t *testing.T) {
			// Call the function being tested
			input := ProcessRawCollectionsInput{
				Columns:        tt.args.columns,
				Values:         tt.args.values,
				TableName:      tt.args.tableName,
				Translator:     tt.args.t,
				KeySpace:       "test_keyspace",
				PrependColumns: nil, // Assuming nil for these tests, adjust if needed
			}
			output, err := processCollectionColumnsForRawQueries(input)

			if (err != nil) != tt.wantErr {
				t.Errorf("processCollectionColumnsForRawQueries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil { // Only check output if no error was expected/occurred
				if !strings.Contains(tt.name, "List") && !reflect.DeepEqual(output.NewColumns, tt.want) {
					t.Errorf("processCollectionColumnsForRawQueries() output.NewColumns = %v, want %v", output.NewColumns, tt.want)
				}
				if !reflect.DeepEqual(output.NewValues, tt.want1) {
					t.Errorf("processCollectionColumnsForRawQueries() output.NewValues = %v, want %v", output.NewValues, tt.want1)
				}
				if !reflect.DeepEqual(output.DelColumnFamily, tt.want2) {
					t.Errorf("processCollectionColumnsForRawQueries() output.DelColumnFamily = %v, want %v", output.DelColumnFamily, tt.want2)
				}
				// Note: tt.want3 (DelColumns) comparison is missing in the original test, adding it might be good practice
				// if !reflect.DeepEqual(output.DelColumns, tt.want3) {
				// 	t.Errorf("processCollectionColumnsForRawQueries() output.DelColumns = %v, want %v", output.DelColumns, tt.want3)
				// }
			}
		})
	}
}

func TestStringToPrimitives(t *testing.T) {
	tests := []struct {
		value    string
		cqlType  string
		expected interface{}
		hasError bool
	}{
		{"123", "int", int32(123), false},
		{"not_an_int", "int", nil, true},
		{"123456789", "bigint", int64(123456789), false},
		{"not_a_bigint", "bigint", nil, true},
		{"3.14", "float", float32(3.14), false},
		{"not_a_float", "float", nil, true},
		{"3.1415926535", "double", float64(3.1415926535), false},
		{"not_a_double", "double", nil, true},
		{"true", "boolean", true, false},
		{"false", "boolean", false, false},
		{"not_a_boolean", "boolean", nil, true},
		{"blob_data", "blob", "blob_data", false},
		{"hello", "text", "hello", false},
		{"123", "unsupported_type", nil, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%s", tt.cqlType, tt.value), func(t *testing.T) {
			result, err := stringToPrimitives(tt.value, tt.cqlType)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error: %v, got error: %v", tt.hasError, err)
			}
			if result != tt.expected {
				t.Errorf("expected result: %v, got result: %v", tt.expected, result)
			}
		})
	}
}

func Test_formatValues(t *testing.T) {
	type args struct {
		value     string
		cqlType   string
		protocolV primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "Invalid int",
			args:    args{"abc", "int", primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid bigint",
			args:    args{"abc", "bigint", primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid float",
			args:    args{"abc", "float", primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid double",
			args:    args{"abc", "double", primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid boolean",
			args:    args{"abc", "boolean", primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid timestamp",
			args:    args{"abc", "timestamp", primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Unsupported type",
			args:    args{"123", "unsupported", primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatValues(tt.args.value, tt.args.cqlType, tt.args.protocolV)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("formatValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processCollectionColumnsForPrepareQueries(t *testing.T) {
	mapTypeTextText := datatype.NewMapType(datatype.Varchar, datatype.Varchar)
	mapTypeTextBool := datatype.NewMapType(datatype.Varchar, datatype.Boolean)
	mapTypeTextInt := datatype.NewMapType(datatype.Varchar, datatype.Int)
	mapTypeTextFloat := datatype.NewMapType(datatype.Varchar, datatype.Float)
	mapTypeTextDouble := datatype.NewMapType(datatype.Varchar, datatype.Double)
	mapTypeTextTimestamp := datatype.NewMapType(datatype.Varchar, datatype.Timestamp)
	mapTypeTimestampBoolean := datatype.NewMapType(datatype.Timestamp, datatype.Boolean)
	mapTypeTimestampText := datatype.NewMapType(datatype.Timestamp, datatype.Varchar)
	mapTypeTimestampInt := datatype.NewMapType(datatype.Timestamp, datatype.Int)
	mapTypeTimestampFloat := datatype.NewMapType(datatype.Timestamp, datatype.Float)
	mapTypeTimestampBigint := datatype.NewMapType(datatype.Timestamp, datatype.Bigint)
	mapTypeTimestampDouble := datatype.NewMapType(datatype.Timestamp, datatype.Double)
	mapTypeTimestampTimestamp := datatype.NewMapType(datatype.Timestamp, datatype.Timestamp)
	mapTypeTextBigint := datatype.NewMapType(datatype.Varchar, datatype.Bigint)
	setTypeBoolean := datatype.NewSetType(datatype.Boolean)
	setTypeInt := datatype.NewSetType(datatype.Int)
	setTypeBigint := datatype.NewSetType(datatype.Bigint)
	setTypeText := datatype.NewSetType(datatype.Varchar)
	setTypeFloat := datatype.NewSetType(datatype.Float)
	setTypeDouble := datatype.NewSetType(datatype.Double)
	setTypeTimestamp := datatype.NewSetType(datatype.Timestamp)

	valuesTextText := map[string]string{"test": "test"}
	textBytesTextText, _ := proxycore.EncodeType(mapTypeTextText, primitive.ProtocolVersion4, valuesTextText)
	textValue, _ := formatValues("test", "varchar", primitive.ProtocolVersion4)
	trueVal, _ := formatValues("true", "boolean", primitive.ProtocolVersion4)

	valuesTextBool := map[string]bool{"test": true}
	textBytesTextBool, _ := proxycore.EncodeType(mapTypeTextBool, primitive.ProtocolVersion4, valuesTextBool)

	valuesTextInt := map[string]int{"test": 42}
	textBytesTextInt, _ := proxycore.EncodeType(mapTypeTextInt, primitive.ProtocolVersion4, valuesTextInt)
	intValue, _ := formatValues("42", "int", primitive.ProtocolVersion4)

	valuesTextFloat := map[string]float32{"test": 3.14}
	textBytesTextFloat, _ := proxycore.EncodeType(mapTypeTextFloat, primitive.ProtocolVersion4, valuesTextFloat)
	floatValue, _ := formatValues("3.14", "float", primitive.ProtocolVersion4)

	valuesTextDouble := map[string]float64{"test": 6.283}
	textBytesTextDouble, _ := proxycore.EncodeType(mapTypeTextDouble, primitive.ProtocolVersion4, valuesTextDouble)
	doubleValue, _ := formatValues("6.283", "double", primitive.ProtocolVersion4)

	valuesTextTimestamp := map[string]time.Time{"test": time.Unix(1633046400, 0)} // Example timestamp
	textBytesTextTimestamp, _ := proxycore.EncodeType(mapTypeTextTimestamp, primitive.ProtocolVersion4, valuesTextTimestamp)
	timestampValue, _ := formatValues("1633046400000", "timestamp", primitive.ProtocolVersion4) // Example in milliseconds

	valuesTimestampBoolean := map[time.Time]bool{
		time.Unix(1633046400, 0): true,
	}
	textBytesTimestampBoolean, _ := proxycore.EncodeType(mapTypeTimestampBoolean, primitive.ProtocolVersion4, valuesTimestampBoolean)
	timestampBooleanValue, _ := formatValues("true", "boolean", primitive.ProtocolVersion4)

	valuesTimestampText := map[time.Time]string{
		time.Unix(1633046400, 0): "example_text", // Example timestamp as key with text value
	}
	textBytesTimestampText, _ := proxycore.EncodeType(mapTypeTimestampText, primitive.ProtocolVersion4, valuesTimestampText)
	timestampTextValue, _ := formatValues("example_text", "varchar", primitive.ProtocolVersion4)

	valuesTimestampInt := map[time.Time]int{
		time.Unix(1633046400, 0): 42, // Example timestamp as key with int value
	}
	textBytesTimestampInt, _ := proxycore.EncodeType(mapTypeTimestampInt, primitive.ProtocolVersion4, valuesTimestampInt)
	timestampIntValue, _ := formatValues("42", "int", primitive.ProtocolVersion4)

	valuesTimestampFloat := map[time.Time]float32{
		time.Unix(1633046400, 0): 3.14, // Example timestamp as key with float value
	}
	textBytesTimestampFloat, _ := proxycore.EncodeType(mapTypeTimestampFloat, primitive.ProtocolVersion4, valuesTimestampFloat)
	timestampFloatValue, _ := formatValues("3.14", "float", primitive.ProtocolVersion4)

	valuesTimestampBigint := map[time.Time]int64{
		time.Unix(1633046400, 0): 1234567890123, // Example timestamp as key with bigint value
	}
	textBytesTimestampBigint, _ := proxycore.EncodeType(mapTypeTimestampBigint, primitive.ProtocolVersion4, valuesTimestampBigint)
	timestampBigintValue, _ := formatValues("1234567890123", "bigint", primitive.ProtocolVersion4)

	valuesTimestampDouble := map[time.Time]float64{
		time.Unix(1633046400, 0): 6.283, // Example timestamp as key with double value
	}
	textBytesTimestampDouble, _ := proxycore.EncodeType(mapTypeTimestampDouble, primitive.ProtocolVersion4, valuesTimestampDouble)
	timestampDoubleValue, _ := formatValues("6.283", "double", primitive.ProtocolVersion4)

	valuesTimestampTimestamp := map[time.Time]time.Time{
		time.Unix(1633046400, 0): time.Unix(1633126400, 0), // Example timestamp as key with timestamp value
	}
	textBytesTimestampTimestamp, _ := proxycore.EncodeType(mapTypeTimestampTimestamp, primitive.ProtocolVersion4, valuesTimestampTimestamp)
	timestampTimestampValue, _ := formatValues("1633126400000", "timestamp", primitive.ProtocolVersion4) // Example in milliseconds

	valuesTextBigint := map[string]int64{"test": 1234567890123}
	textBytesTextBigint, _ := proxycore.EncodeType(mapTypeTextBigint, primitive.ProtocolVersion4, valuesTextBigint)
	bigintValue, _ := formatValues("1234567890123", "bigint", primitive.ProtocolVersion4)

	valuesSetBoolean := []bool{true}
	valuesSetInt := []int32{12}
	valuesSetBigInt := []int64{12372432764}
	valuesSetText := []string{"test"}
	valuesSetFloat := []float32{6.283}
	valuesSetDouble := []float64{6.283}
	valuesSetTimestamp := []int64{1633046400}

	setBytesBoolean, _ := proxycore.EncodeType(setTypeBoolean, primitive.ProtocolVersion4, valuesSetBoolean)
	setBytesInt, _ := proxycore.EncodeType(setTypeInt, primitive.ProtocolVersion4, valuesSetInt)
	setBytesBigInt, _ := proxycore.EncodeType(setTypeBigint, primitive.ProtocolVersion4, valuesSetBigInt)
	setBytesText, _ := proxycore.EncodeType(setTypeText, primitive.ProtocolVersion4, valuesSetText)
	setBytesFloat, _ := proxycore.EncodeType(setTypeFloat, primitive.ProtocolVersion4, valuesSetFloat)
	setBytesDouble, _ := proxycore.EncodeType(setTypeDouble, primitive.ProtocolVersion4, valuesSetDouble)
	setBytesTimestamp, _ := proxycore.EncodeType(setTypeTimestamp, primitive.ProtocolVersion4, valuesSetTimestamp)

	emptyVal, _ := formatValues("", "varchar", primitive.ProtocolVersion4)
	listTextType := datatype.NewListType(datatype.Varchar)
	valuesListText := []string{"test"}
	listBytesText, _ := proxycore.EncodeType(listTextType, primitive.ProtocolVersion4, valuesListText)

	listIntType := datatype.NewListType(datatype.Int)
	valuesListInt := []int32{42}
	listBytesInt, _ := proxycore.EncodeType(listIntType, primitive.ProtocolVersion4, valuesListInt)

	listBigintType := datatype.NewListType(datatype.Bigint)
	valuesListBigint := []int64{1234567890123}
	listBytesBigint, _ := proxycore.EncodeType(listBigintType, primitive.ProtocolVersion4, valuesListBigint)

	listBoolType := datatype.NewListType(datatype.Boolean)
	valuesListBool := []bool{true}
	listBytesBool, _ := proxycore.EncodeType(listBoolType, primitive.ProtocolVersion4, valuesListBool)

	listDoubleType := datatype.NewListType(datatype.Double)
	valuesListDouble := []float64{6.283}
	listBytesDouble, _ := proxycore.EncodeType(listDoubleType, primitive.ProtocolVersion4, valuesListDouble)

	listFloatType := datatype.NewListType(datatype.Float)
	valuesListFloat := []float32{3.14}
	listBytesFloat, _ := proxycore.EncodeType(listFloatType, primitive.ProtocolVersion4, valuesListFloat)

	listTimestampType := datatype.NewListType(datatype.Timestamp)
	valuesListTimestamp := []int64{1633046400000}
	listBytesTimestamp, _ := proxycore.EncodeType(listTimestampType, primitive.ProtocolVersion4, valuesListTimestamp)

	floatVal, _ := formatValues("3.14", "float", primitive.ProtocolVersion4)
	doubleVal, _ := formatValues("6.283", "double", primitive.ProtocolVersion4)
	timestampVal, _ := formatValues("1633046400000", "timestamp", primitive.ProtocolVersion4)

	tests := []struct {
		name             string
		columns          []Column
		variableMetadata []*message.ColumnMetadata
		values           []*primitive.Value
		tableName        string
		protocolV        primitive.ProtocolVersion
		primaryKeys      []string
		translator       *Translator
		want             []Column
		want1            []interface{}
		want2            map[string]interface{}
		want3            int
		want4            []string
		wantErr          bool
	}{
		{
			name: "Valid Input For Timestamp Float",
			columns: []Column{
				{Name: "map_timestamp_float", ColumnFamily: "map_timestamp_float", CQLType: "map<timestamp,float>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampFloat},
			},

			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400000", ColumnFamily: "map_timestamp_float", CQLType: "float"},
			},
			want1:   []interface{}{timestampFloatValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_timestamp_float"},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Timestamp",
			columns: []Column{
				{Name: "map_text_timestamp", ColumnFamily: "map_text_timestamp", CQLType: "map<varchar,timestamp>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTextTimestamp},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_timestamp", CQLType: "bigint"},
			},
			want1:   []interface{}{timestampValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_text_timestamp"},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Text",
			columns: []Column{
				{Name: "map_timestamp_text", ColumnFamily: "map_timestamp_text", CQLType: "map<timestamp,varchar>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampText},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400000", ColumnFamily: "map_timestamp_text", CQLType: "varchar"},
			},
			want1:   []interface{}{timestampTextValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_timestamp_text"},
			wantErr: false,
		},
		{
			name: "Valid Input For Set Timestamp",
			columns: []Column{
				{Name: "set_timestamp", ColumnFamily: "set_timestamp", CQLType: "set<timestamp>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesTimestamp},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400", ColumnFamily: "set_timestamp", CQLType: "bigint"},
			},
			want1:   []interface{}{emptyVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"set_timestamp"},
			wantErr: false,
		},
		{
			name: "Valid Input For Set Double",
			columns: []Column{
				{Name: "set_double", ColumnFamily: "set_double", CQLType: "set<double>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesDouble},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "6.283", ColumnFamily: "set_double", CQLType: "double"},
			},
			want1:   []interface{}{emptyVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"set_double"},
			wantErr: false,
		},
		{
			name: "Valid Input For Set Float",
			columns: []Column{
				{Name: "set_float", ColumnFamily: "set_float", CQLType: "set<float>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesFloat},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "6.283", ColumnFamily: "set_float", CQLType: "float"},
			},
			want1:   []interface{}{emptyVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"set_float"},
			wantErr: false,
		},
		{
			name: "Valid Input For Set Text",
			columns: []Column{
				{Name: "set_text", ColumnFamily: "set_text", CQLType: "set<varchar>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesText},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "set_text", CQLType: "varchar"},
			},
			want1:   []interface{}{emptyVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"set_text"},
			wantErr: false,
		},
		{
			name: "Valid Input For Set BigInt",
			columns: []Column{
				{Name: "set_bigint", ColumnFamily: "set_bigint", CQLType: "set<bigint>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesBigInt},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "12372432764", ColumnFamily: "set_bigint", CQLType: "bigint"},
			},
			want1:   []interface{}{emptyVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"set_bigint"},
			wantErr: false,
		},
		{
			name: "Valid Input For Set Int",
			columns: []Column{
				{Name: "set_int", ColumnFamily: "set_int", CQLType: "set<int>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesInt},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "12", ColumnFamily: "set_int", CQLType: "int"},
			},
			want1:   []interface{}{emptyVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"set_int"},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Timestamp",
			columns: []Column{
				{Name: "map_timestamp_timestamp", ColumnFamily: "map_timestamp_timestamp", CQLType: "map<timestamp,timestamp>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampTimestamp},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400000", ColumnFamily: "map_timestamp_timestamp", CQLType: "bigint"},
			},
			want1:   []interface{}{timestampTimestampValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_timestamp_timestamp"},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Bigint",
			columns: []Column{
				{Name: "map_text_bigint", ColumnFamily: "map_text_bigint", CQLType: "map<varchar,bigint>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTextBigint},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_bigint", CQLType: "bigint"},
			},
			want1:   []interface{}{bigintValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_text_bigint"},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Int",
			columns: []Column{
				{Name: "map_timestamp_int", ColumnFamily: "map_timestamp_int", CQLType: "map<timestamp,int>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampInt},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400000", ColumnFamily: "map_timestamp_int", CQLType: "int"},
			},
			want1:   []interface{}{timestampIntValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_timestamp_int"},
			wantErr: false,
		},
		{
			name: "Valid Input For Set Boolean",
			columns: []Column{
				{Name: "set_boolean", ColumnFamily: "set_boolean", CQLType: "set<boolean>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesBoolean},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1", ColumnFamily: "set_boolean", CQLType: "boolean"},
			},
			want1:   []interface{}{emptyVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"set_boolean"},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Boolean",
			columns: []Column{
				{Name: "map_text_boolean", ColumnFamily: "map_text_boolean", CQLType: "map<varchar,boolean>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTextBool},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_boolean", CQLType: "boolean"},
			},
			want1:   []interface{}{trueVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_text_boolean"},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Text",
			columns: []Column{
				{Name: "map_text_text", ColumnFamily: "map_text_text", CQLType: "map<varchar,varchar>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTextText},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_text", CQLType: "varchar"},
			},
			want1:   []interface{}{textValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_text_text"},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Int",
			columns: []Column{
				{Name: "map_text_int", ColumnFamily: "map_text_int", CQLType: "map<varchar,int>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTextInt},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_int", CQLType: "int"},
			},
			want1:   []interface{}{intValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_text_int"},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Float",
			columns: []Column{
				{Name: "map_text_float", ColumnFamily: "map_text_float", CQLType: "map<varchar,float>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTextFloat},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_float", CQLType: "float"},
			},
			want1:   []interface{}{floatValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_text_float"},
			wantErr: false,
		},
		{
			name: "Valid Input For Text Double",
			columns: []Column{
				{Name: "map_text_double", ColumnFamily: "map_text_double", CQLType: "map<varchar,double>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTextDouble},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "test", ColumnFamily: "map_text_double", CQLType: "double"},
			},
			want1:   []interface{}{doubleValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_text_double"},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Boolean",
			columns: []Column{
				{Name: "map_timestamp_boolean", ColumnFamily: "map_timestamp_boolean", CQLType: "map<timestamp,boolean>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampBoolean},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400000", ColumnFamily: "map_timestamp_boolean", CQLType: "boolean"},
			},
			want1:   []interface{}{timestampBooleanValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_timestamp_boolean"},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Double",
			columns: []Column{
				{Name: "map_timestamp_double", ColumnFamily: "map_timestamp_double", CQLType: "map<timestamp,double>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampDouble},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400000", ColumnFamily: "map_timestamp_double", CQLType: "double"},
			},
			want1:   []interface{}{timestampDoubleValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_timestamp_double"},
			wantErr: false,
		},
		{
			name: "Valid Input For Timestamp Bigint",
			columns: []Column{
				{Name: "map_timestamp_bigint", ColumnFamily: "map_timestamp_bigint", CQLType: "map<timestamp,bigint>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampBigint},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: "1633046400000", ColumnFamily: "map_timestamp_bigint", CQLType: "bigint"},
			},
			want1:   []interface{}{timestampBigintValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"map_timestamp_bigint"},
			wantErr: false,
		},
		{
			name: "Invalid Input For Set Bigint",
			columns: []Column{
				{Name: "set_bigint", ColumnFamily: "set_bigint", CQLType: "set<bigint>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesBoolean}, // Invalid type
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			want3:   0,
			want4:   nil,
			wantErr: true,
		},
		{
			name: "Invalid Input For Text Bigint",
			columns: []Column{
				{Name: "map_text_bigint", ColumnFamily: "map_text_bigint", CQLType: "map<varchar,bigint>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: textBytesTimestampBoolean},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			want3:   0,
			want4:   nil,
			wantErr: true,
		},
		{
			name: "Invalid Input For Set Int",
			columns: []Column{
				{Name: "set_int", ColumnFamily: "set_int", CQLType: "set<int>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesBoolean}, // Invalid type
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			want3:   0,
			want4:   nil,
			wantErr: true,
		},
		{
			name: "Invalid Input For Set Float",
			columns: []Column{
				{Name: "set_float", ColumnFamily: "set_float", CQLType: "set<float>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesBoolean},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			want3:   0,
			want4:   nil,
			wantErr: true,
		},
		{
			name: "Invalid Input For Set Double",
			columns: []Column{
				{Name: "set_double", ColumnFamily: "set_double", CQLType: "set<double>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesBoolean},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			want3:   0,
			want4:   nil,
			wantErr: true,
		},
		{
			name: "Invalid Input For Set Timestamp",
			columns: []Column{
				{Name: "set_timestamp", ColumnFamily: "set_timestamp", CQLType: "set<timestamp>"},
			},
			variableMetadata: []*message.ColumnMetadata{},
			values: []*primitive.Value{
				{Contents: setBytesBoolean},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			want3:   0,
			want4:   nil,
			wantErr: true,
		},
		{
			name: "Valid Input For List<text>",
			columns: []Column{
				{Name: "list_text", ColumnFamily: "list_text", CQLType: "list<varchar>"},
			},
			values: []*primitive.Value{
				{Contents: listBytesText},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: time.Now().Format("20060102150405.000"), ColumnFamily: "list_text", CQLType: "varchar"},
			},
			want1:   []interface{}{textValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"list_text"},
			wantErr: false,
		},
		{
			name: "Valid Input For List<int>",
			columns: []Column{
				{Name: "list_int", ColumnFamily: "list_int", CQLType: "list<int>"},
			},
			values: []*primitive.Value{
				{Contents: listBytesInt},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: time.Now().Format("20060102150405.000"), ColumnFamily: "list_int", CQLType: "int"},
			},
			want1:   []interface{}{intValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"list_int"},
			wantErr: false,
		},
		{
			name: "Valid Input For List<bigint>",
			columns: []Column{
				{Name: "list_bigint", ColumnFamily: "list_bigint", CQLType: "list<bigint>"},
			},
			values: []*primitive.Value{
				{Contents: listBytesBigint},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: time.Now().Format("20060102150405.000"), ColumnFamily: "list_bigint", CQLType: "bigint"},
			},
			want1:   []interface{}{bigintValue},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"list_bigint"},
			wantErr: false,
		},
		{
			name: "Valid Input For List<boolean>",
			columns: []Column{
				{Name: "list_boolean", ColumnFamily: "list_boolean", CQLType: "list<boolean>"},
			},
			values: []*primitive.Value{
				{Contents: listBytesBool},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: time.Now().Format("20060102150405.000"), ColumnFamily: "list_boolean", CQLType: "boolean"},
			},
			want1:   []interface{}{trueVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"list_boolean"},
			wantErr: false,
		},
		{
			name: "Valid Input For List<float>",
			columns: []Column{
				{Name: "list_float", ColumnFamily: "list_float", CQLType: "list<float>"},
			},
			values: []*primitive.Value{
				{Contents: listBytesFloat},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: time.Now().Format("20060102150405.000"), ColumnFamily: "list_float", CQLType: "float"},
			},
			want1:   []interface{}{floatVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"list_float"},
			wantErr: false,
		},
		{
			name: "Valid Input For List<double>",
			columns: []Column{
				{Name: "list_double", ColumnFamily: "list_double", CQLType: "list<double>"},
			},
			values: []*primitive.Value{
				{Contents: listBytesDouble},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: time.Now().Format("20060102150405.000"), ColumnFamily: "list_double", CQLType: "double"},
			},
			want1:   []interface{}{doubleVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"list_double"},
			wantErr: false,
		},
		{
			name: "Valid Input For List<timestamp>",
			columns: []Column{
				{Name: "list_timestamp", ColumnFamily: "list_timestamp", CQLType: "list<timestamp>"},
			},
			values: []*primitive.Value{
				{Contents: listBytesTimestamp},
			},
			tableName:   "non_primitive_table",
			protocolV:   primitive.ProtocolVersion4,
			primaryKeys: []string{},
			translator: &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: []Column{
				{Name: time.Now().Format("20060102150405.000"), ColumnFamily: "list_timestamp", CQLType: "bigint"},
			},
			want1:   []interface{}{timestampVal},
			want2:   map[string]interface{}{},
			want3:   0,
			want4:   []string{"list_timestamp"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := ProcessPrepareCollectionsInput{
				ColumnsResponse: tt.columns,
				Values:          tt.values,
				TableName:       tt.tableName,
				ProtocolV:       tt.protocolV,
				PrimaryKeys:     tt.primaryKeys,
				Translator:      tt.translator,
				KeySpace:        "test_keyspace",
				ComplexMeta:     nil, // Assuming nil for these tests, adjust if needed
			}
			output, err := processCollectionColumnsForPrepareQueries(input)

			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return // Don't check results if an error was expected
			}
			// For list types, normalize Names for comparison as its a timestamp value based on time.Now()
			if strings.Contains(tt.name, "List") {
				// Normalize both output and expected Names for comparison
				for i := range output.NewColumns {
					output.NewColumns[i].Name = fmt.Sprintf("list_index_%d", i)
				}
				for i := range tt.want {
					tt.want[i].Name = fmt.Sprintf("list_index_%d", i)
				}
			}

			if !reflect.DeepEqual(output.NewColumns, tt.want) {
				t.Errorf("output.NewColumns = %v, want %v", output.NewColumns, tt.want)
			}
			if !reflect.DeepEqual(output.NewValues, tt.want1) {
				t.Errorf("output.NewValues = %v, want %v", output.NewValues, tt.want1)
			}
			if !reflect.DeepEqual(output.Unencrypted, tt.want2) {
				t.Errorf("output.Unencrypted = %v, want %v", output.Unencrypted, tt.want2)
			}
			if output.IndexEnd != tt.want3 {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.IndexEnd = %v, want %v", output.IndexEnd, tt.want3)
			}
			if !reflect.DeepEqual(output.DelColumnFamily, tt.want4) {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.DelColumnFamily = %v, want %v", output.DelColumnFamily, tt.want4)
			}
		})
	}
}

func TestConvertToBigtableTimestamp(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    TimestampInfo
		expectError bool
	}{
		{
			name:  "Valid timestamp input in nano second",
			input: "1634232345000000",
			expected: TimestampInfo{
				Timestamp:         bigtable.Time(time.Unix(1634232345, 0)),
				HasUsingTimestamp: true,
				Index:             0,
			},
			expectError: false,
		},
		{
			name:  "Valid timestamp input in micro second",
			input: "1634232345000",
			expected: TimestampInfo{
				Timestamp:         bigtable.Time(time.Unix(1634232345, 0)),
				HasUsingTimestamp: true,
				Index:             0,
			},
			expectError: false,
		},
		{
			name:  "Valid timestamp input in seconds",
			input: "1634232345",
			expected: TimestampInfo{
				Timestamp:         bigtable.Time(time.Unix(1634232345, 0)),
				HasUsingTimestamp: true,
				Index:             0,
			},
			expectError: false,
		},
		{
			name:  "Empty input",
			input: "",
			expected: TimestampInfo{
				Timestamp:         bigtable.Timestamp(0),
				HasUsingTimestamp: true,
				Index:             0,
			},
			expectError: false,
		},
		{
			name:  "Input contains question mark",
			input: "1634232345?",
			expected: TimestampInfo{
				Timestamp:         bigtable.Timestamp(0),
				HasUsingTimestamp: true,
				Index:             0,
			},
			expectError: false,
		},
		{
			name:        "Invalid input",
			input:       "invalid",
			expected:    TimestampInfo{},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := convertToBigtableTimestamp(test.input, 0)

			if (err != nil) != test.expectError {
				t.Errorf("Unexpected error status: got %v, expected error %v", err, test.expectError)
			}

			if !test.expectError && result != test.expected {
				t.Errorf("Unexpected result: got %+v, expected %+v", result, test.expected)
			}
		})
	}
}

func Test_validateRequiredPrimaryKeys(t *testing.T) {
	type args struct {
		requiredKey []string
		actualKey   []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Equal slices with different order",
			args: args{
				requiredKey: []string{"key1", "key2", "key3"},
				actualKey:   []string{"key3", "key2", "key1"},
			},
			want: true,
		},
		{
			name: "Equal slices with same order",
			args: args{
				requiredKey: []string{"key1", "key2", "key3"},
				actualKey:   []string{"key1", "key2", "key3"},
			},
			want: true,
		},
		{
			name: "Unequal slices with different elements",
			args: args{
				requiredKey: []string{"key1", "key2", "key3"},
				actualKey:   []string{"key1", "key4", "key3"},
			},
			want: false,
		},
		{
			name: "Unequal slices with different lengths",
			args: args{
				requiredKey: []string{"key1", "key2"},
				actualKey:   []string{"key1", "key2", "key3"},
			},
			want: false,
		},
		{
			name: "Both slices empty",
			args: args{
				requiredKey: []string{},
				actualKey:   []string{},
			},
			want: true,
		},
		{
			name: "One slice empty, one not",
			args: args{
				requiredKey: []string{"key1"},
				actualKey:   []string{},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateRequiredPrimaryKeys(tt.args.requiredKey, tt.args.actualKey); got != tt.want {
				t.Errorf("validateRequiredPrimaryKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessComplexUpdate(t *testing.T) {
	translator := &Translator{
		SchemaMappingConfig: GetSchemaMappingConfig(),
	}

	tests := []struct {
		name           string
		columns        []Column
		values         []interface{}
		prependColumns []string
		expectedMeta   map[string]*ComplexOperation
		expectedErr    error
	}{
		{
			name: "successful collection update for map and list",
			columns: []Column{
				{Name: "map_text_bool_col", CQLType: "map<varchar, boolean>"},
				{Name: "list_text", CQLType: "list<varchar>"},
			},
			values: []interface{}{
				"map_text_bool_col+{key:?}",
				"list_text+?",
			},
			prependColumns: []string{"list_text"},
			expectedMeta: map[string]*ComplexOperation{
				"map_text_bool_col": {
					Append:           true,
					mapKey:           "key",
					ExpectedDatatype: datatype.Boolean,
				},
				"list_text": {
					mapKey:      "",
					Append:      false,
					PrependList: true,
				},
			},
			expectedErr: nil,
		},
		{
			name: "non-collection column should be skipped",
			columns: []Column{
				{Name: "pk_1_text", CQLType: "varchar"},
			},
			values: []interface{}{
				"pk_1_text+value",
			},
			prependColumns: []string{},
			expectedMeta:   map[string]*ComplexOperation{},
			expectedErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			complexMeta, err := translator.ProcessComplexUpdate(tt.columns, tt.values, "test_table", "test_keyspace", tt.prependColumns)

			if err != tt.expectedErr {
				t.Errorf("expected error %v, got %v", tt.expectedErr, err)
			}

			if len(complexMeta) != len(tt.expectedMeta) {
				t.Errorf("expected length %d, got %d", len(tt.expectedMeta), len(complexMeta))
			}

			for key, expectedComplexUpdate := range tt.expectedMeta {
				actualComplexUpdate, exists := complexMeta[key]
				if !exists {
					t.Errorf("expected key %s to exist in result", key)
				} else {
					if !compareComplexOperation(expectedComplexUpdate, actualComplexUpdate) {
						t.Errorf("expected meta for key %s: %v, got: %v", key, expectedComplexUpdate, actualComplexUpdate)
					}
				}
			}
		})
	}
}

func TestExtractWritetimeValue(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 bool
	}{
		{
			name:  "Valid writetime statement",
			args:  args{s: "writetime(column)"},
			want:  "column",
			want1: true,
		},
		{
			name:  "Invalid missing closing parenthesis",
			args:  args{s: "writetime(column"},
			want:  "",
			want1: false,
		},
		{
			name:  "Invalid missing opening parenthesis",
			args:  args{s: "writetime)"},
			want:  "",
			want1: false,
		},
		{
			name:  "Completely invalid string",
			args:  args{s: "some random string"},
			want:  "",
			want1: false,
		},
		{
			name:  "Empty string",
			args:  args{s: ""},
			want:  "",
			want1: false,
		},
		{
			name:  "Case insensitivity",
			args:  args{s: "WriteTime(test)"},
			want:  "test",
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := ExtractWritetimeValue(tt.args.s)
			if got != tt.want {
				t.Errorf("ExtractWritetimeValue() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ExtractWritetimeValue() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestTransformPrepareQueryForPrependList(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectedQuery   string
		expectedColumns []string
	}{
		{
			name:            "No WHERE clause",
			query:           "UPDATE table SET pk_1_text = ?",
			expectedQuery:   "UPDATE table SET pk_1_text = ?",
			expectedColumns: nil,
		},
		{
			name:            "No prepend operation",
			query:           "UPDATE table SET pk_1_text = ? WHERE id = ?",
			expectedQuery:   "UPDATE table SET pk_1_text = ? WHERE id = ?",
			expectedColumns: nil,
		},
		{
			name:            "Prepend operation detected",
			query:           "UPDATE table SET pk_1_text = ? + pk_1_text WHERE id = ?",
			expectedQuery:   "UPDATE table SET pk_1_text = pk_1_text + ? WHERE id = ?",
			expectedColumns: []string{"pk_1_text"},
		},
		{
			name:            "Multiple assignments with prepend",
			query:           "UPDATE table SET pk_1_text = ? + pk_1_text, blob_col = ? WHERE id = ?",
			expectedQuery:   "UPDATE table SET pk_1_text = pk_1_text + ?, blob_col = ? WHERE id = ?",
			expectedColumns: []string{"pk_1_text"},
		},
		{
			name:            "Multiple prepend operations",
			query:           "UPDATE table SET pk_1_text = ? + pk_1_text, blob_col = ? + blob_col WHERE id = ?",
			expectedQuery:   "UPDATE table SET pk_1_text = pk_1_text + ?, blob_col = blob_col + ? WHERE id = ?",
			expectedColumns: []string{"pk_1_text", "blob_col"},
		},
		{
			name:            "Prepend operation with different variable names",
			query:           "UPDATE table SET pk_1_text = ? + blob_col WHERE id = ?",
			expectedQuery:   "UPDATE table SET pk_1_text = ? + blob_col WHERE id = ?",
			expectedColumns: nil,
		},
		{
			name:            "Prepend operation with quotes",
			query:           `UPDATE table SET pk_1_text = '?' + pk_1_text WHERE id = ?`,
			expectedQuery:   `UPDATE table SET pk_1_text = pk_1_text + '?' WHERE id = ?`,
			expectedColumns: []string{"pk_1_text"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modifiedQuery, columns := TransformPrepareQueryForPrependList(tt.query)
			if modifiedQuery != tt.expectedQuery {
				t.Errorf("TransformPrepareQueryForPrependList() modifiedQuery = %v, want %v", modifiedQuery, tt.expectedQuery)
			}
			if !reflect.DeepEqual(columns, tt.expectedColumns) {
				t.Errorf("TransformPrepareQueryForPrependList() columns = %v, want %v", columns, tt.expectedColumns)
			}
		})
	}
}

func TestCastColumns(t *testing.T) {
	tests := []struct {
		name         string
		colMeta      *schemaMapping.Column
		columnFamily string
		want         string
		wantErr      bool
	}{
		{
			name: "integer type",
			colMeta: &schemaMapping.Column{
				ColumnName: "age",
				ColumnType: "int",
			},
			columnFamily: "cf1",
			want:         "TO_INT64(cf1['age'])",
			wantErr:      false,
		},
		{
			name: "bigint type",
			colMeta: &schemaMapping.Column{
				ColumnName: "timestamp",
				ColumnType: "bigint",
			},
			columnFamily: "cf1",
			want:         "TO_INT64(cf1['timestamp'])",
			wantErr:      false,
		},
		{
			name: "float type",
			colMeta: &schemaMapping.Column{
				ColumnName: "price",
				ColumnType: "float",
			},
			columnFamily: "cf1",
			want:         "TO_FLOAT32(cf1['price'])",
			wantErr:      false,
		},
		{
			name: "double type",
			colMeta: &schemaMapping.Column{
				ColumnName: "value",
				ColumnType: "double",
			},
			columnFamily: "cf1",
			want:         "TO_FLOAT64(cf1['value'])",
			wantErr:      false,
		},
		{
			name: "boolean type",
			colMeta: &schemaMapping.Column{
				ColumnName: "active",
				ColumnType: "boolean",
			},
			columnFamily: "cf1",
			want:         "TO_INT64(cf1['active'])",
			wantErr:      false,
		},
		{
			name: "timestamp type",
			colMeta: &schemaMapping.Column{
				ColumnName: "created_at",
				ColumnType: "timestamp",
			},
			columnFamily: "cf1",
			want:         "TO_TIME(cf1['created_at'])",
			wantErr:      false,
		},
		{
			name: "blob type",
			colMeta: &schemaMapping.Column{
				ColumnName: "data",
				ColumnType: "blob",
			},
			columnFamily: "cf1",
			want:         "TO_BLOB(cf1['data'])",
			wantErr:      false,
		},
		{
			name: "text type",
			colMeta: &schemaMapping.Column{
				ColumnName: "name",
				ColumnType: "text",
			},
			columnFamily: "cf1",
			want:         "cf1['name']",
			wantErr:      false,
		},
		{
			name: "unsupported type",
			colMeta: &schemaMapping.Column{
				ColumnName: "unsupported",
				ColumnType: "unknown_type",
			},
			columnFamily: "cf1",
			want:         "",
			wantErr:      true,
		},
		{
			name: "handle special characters in column name",
			colMeta: &schemaMapping.Column{
				ColumnName: "special-name",
				ColumnType: "text",
			},
			columnFamily: "cf1",
			want:         "cf1['special-name']",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := castColumns(tt.colMeta, tt.columnFamily)
			if (err != nil) != tt.wantErr {
				t.Errorf("castColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("castColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

// compareComplexOperation checks if two ComplexOperation structures are equal.
func compareComplexOperation(expected, actual *ComplexOperation) bool {
	return expected.Append == actual.Append &&
		expected.mapKey == actual.mapKey &&
		expected.PrependList == actual.PrependList &&
		expected.UpdateListIndex == actual.UpdateListIndex &&
		expected.Delete == actual.Delete &&
		expected.ListDelete == actual.ListDelete &&
		reflect.DeepEqual(expected.ExpectedDatatype, actual.ExpectedDatatype)
}

func TestTranslator_CreateOrderedCodeKey(t *testing.T) {
	tests := []struct {
		name                         string
		pmks                         []schemaMapping.Column
		values                       map[string]interface{}
		want                         []byte
		encodeIntValuesWithBigEndian bool
		wantErr                      bool
	}{
		{
			name: "simple string",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": "user1",
			},
			want:                         []byte("user1"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "int nonzero",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "bigint",
					ColumnName:   "user_id",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int64(1),
			},
			want:                         []byte("\x81"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "int32 nonzero",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "int",
					ColumnName:   "user_id",
					ColumnType:   "int",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int32(1),
			},
			want:                         []byte("\x81"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "int32 nonzero big endian",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "int",
					ColumnName:   "user_id",
					ColumnType:   "int",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int32(1),
			},
			want:                         []byte("\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x01"),
			encodeIntValuesWithBigEndian: true,
			wantErr:                      false,
		},
		{
			name: "int32 max",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "int",
					ColumnName:   "user_id",
					ColumnType:   "int",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int32(2147483647),
			},
			want:                         []byte("\x00\xff\x00\xff\x00\xff\x00\xff\x7f\xff\xff\xff"),
			encodeIntValuesWithBigEndian: true,
			wantErr:                      false,
		},
		{
			name: "int64 max",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "bigint",
					ColumnName:   "user_id",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int64(9223372036854775807),
			},
			want:                         []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
			encodeIntValuesWithBigEndian: true,
			wantErr:                      false,
		},
		{
			name: "negative int",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "bigint",
					ColumnName:   "user_id",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int64(-1),
			},
			want:                         []byte("\x7f"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "negative int big endian fails",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "bigint",
					ColumnName:   "user_id",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int64(-1),
			},
			want:                         nil,
			encodeIntValuesWithBigEndian: true,
			wantErr:                      true,
		},
		{
			name: "int zero",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "bigint",
					ColumnName:   "user_id",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int64(0),
			},
			want:                         []byte("\x80"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "int zero big endian",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "bigint",
					ColumnName:   "user_id",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": int64(0),
			},
			want:                         []byte("\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff"),
			encodeIntValuesWithBigEndian: true,
			wantErr:                      false,
		},
		{
			name: "compound key",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "bigint",
					ColumnName:   "team_num",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "city",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
			},
			values: map[string]interface{}{
				"user_id":  "user1",
				"team_num": int64(1),
				"city":     "new york",
			},
			want:                         []byte("user1\x00\x01\x81\x00\x01new york"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "compound key big endian",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "bigint",
					ColumnName:   "team_num",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "city",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
			},
			values: map[string]interface{}{
				"user_id":  "user1",
				"team_num": int64(1),
				"city":     "new york",
			},
			want:                         []byte("user1\x00\x01\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x01\x00\x01new york"),
			encodeIntValuesWithBigEndian: true,
			wantErr:                      false,
		},
		{
			name: "compound key with trailing empty",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "bigint",
					ColumnName:   "team_num",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "city",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "borough",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 4,
				},
			},
			values: map[string]interface{}{
				"user_id":  "user3",
				"team_num": int64(3),
				"city":     "",
				"borough":  "",
			},
			want:                         []byte("user3\x00\x01\x83"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "compound key with trailing empty big endian",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "bigint",
					ColumnName:   "team_num",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "city",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "borough",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 4,
				},
			},
			values: map[string]interface{}{
				"user_id":  "user3",
				"team_num": int64(3),
				"city":     "",
				"borough":  "",
			},
			want:                         []byte("user3\x00\x01\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x03"),
			encodeIntValuesWithBigEndian: true,
			wantErr:                      false,
		},
		{
			name: "compound key with empty middle",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "blob",
					ColumnName:   "user_id",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "blob",
					ColumnName:   "team_id",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "blob",
					ColumnName:   "city",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
			},
			values: map[string]interface{}{
				"user_id": "\xa2",
				"team_id": "",
				"city":    "\xb7",
			},
			want:                         []byte("\xa2\x00\x01\x00\x00\x00\x01\xb7"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "bytes with delimiter",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "blob",
					ColumnName:   "user_id",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": "\x80\x00\x01\x81",
			},
			want:                         []byte("\x80\x00\xff\x01\x81"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "compound key with 2 empty middle fields",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "blob",
					ColumnName:   "user_id",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "blob",
					ColumnName:   "team_num",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "blob",
					ColumnName:   "city",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
				{
					CQLType:      "blob",
					ColumnName:   "borough",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 4,
				},
			},
			values: map[string]interface{}{
				"user_id":  "\xa2",
				"team_num": "",
				"city":     "",
				"borough":  "\xb7",
			},
			want:                         []byte("\xa2\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\xb7"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "byte strings",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "blob",
					ColumnName:   "user_id",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "blob",
					ColumnName:   "city",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
			},
			values: map[string]interface{}{
				"user_id": "\xa5",
				"city":    "\x90",
			},
			want:                         []byte("\xa5\x00\x01\x90"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "empty first value",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "blob",
					ColumnName:   "city",
					ColumnType:   "blob",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
			},
			values: map[string]interface{}{
				"user_id": "",
				"city":    "\xaa",
			},
			want:                         []byte("\x00\x00\x00\x01\xaa"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "null escaped",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "city",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "borough",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
			},
			values: map[string]interface{}{
				"user_id": "nn",
				"city":    "t\x00t",
				"borough": "end",
			},
			want:                         []byte("nn\x00\x01t\x00\xfft\x00\x01end"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
		{
			name: "null escaped",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
				{
					CQLType:      "bigint",
					ColumnName:   "team_num",
					ColumnType:   "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 2,
				},
				{
					CQLType:      "varchar",
					ColumnName:   "city",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 3,
				},
			},
			values: map[string]interface{}{
				"user_id":  "abcd",
				"team_num": int64(45),
				"city":     "name",
			},
			want:                         []byte("abcd\x00\x01\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x2d\x00\x01name"),
			encodeIntValuesWithBigEndian: true,
			wantErr:                      false,
		},
		{
			name: "invalid utf8 varchar returns error",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": string([]uint8{182}),
			},
			want:                         nil,
			encodeIntValuesWithBigEndian: true,
			wantErr:                      true,
		},
		{
			name: "null char",
			pmks: []schemaMapping.Column{
				{
					CQLType:      "varchar",
					ColumnName:   "user_id",
					ColumnType:   "varchar",
					IsPrimaryKey: true,
					PkPrecedence: 1,
				},
			},
			values: map[string]interface{}{
				"user_id": "\x00\x01",
			},
			want:                         []byte("\x00\xff\x01"),
			encodeIntValuesWithBigEndian: false,
			wantErr:                      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createOrderedCodeKey(tt.pmks, tt.values, tt.encodeIntValuesWithBigEndian)
			if (err != nil) != tt.wantErr {
				t.Errorf("createOrderedCodeKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEncodeBool(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			value interface{}
			pv    primitive.ProtocolVersion
		}
		want    []byte
		wantErr bool
	}{
		{
			name: "Valid string 'true'",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "true",
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1},
			wantErr: false,
		},
		{
			name: "Valid string 'false'",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "false",
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name: "String parsing error",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "notabool",
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Valid bool true",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: true,
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1},
			wantErr: false,
		},
		{
			name: "Valid bool false",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: false,
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name: "Valid []byte input for true",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: []byte{1},
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1},
			wantErr: false,
		},
		{
			name: "Valid []byte input for false",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: []byte{0},
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name: "Unsupported type",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: 123,
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EncodeBool(tt.args.value, tt.args.pv)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeBool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEncodeInt(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			value interface{}
			pv    primitive.ProtocolVersion
		}
		want    []byte
		wantErr bool
	}{
		{
			name: "Valid string input",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "12",
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Replace with the bytes you expect
			wantErr: false,
		},
		{
			name: "String parsing error",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "abc",
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Valid int32 input",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: int32(12),
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Replace with the bytes you expect
			wantErr: false,
		},
		{
			name: "Valid []byte input",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: []byte{0, 0, 0, 12}, // Replace with actual bytes representing an int32 value
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Replace with the bytes you expect
			wantErr: false,
		},
		{
			name: "Unsupported type",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: 12.34, // Unsupported float64 type.
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EncodeBigInt(tt.args.value, tt.args.pv)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeBigInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeBigInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataConversionInInsertionIfRequired(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			value        interface{}
			pv           primitive.ProtocolVersion
			cqlType      string
			responseType string
		}
		want    interface{}
		wantErr bool
	}{
		{
			name: "Boolean to string true",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "true",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "string",
			},
			want:    "1",
			wantErr: false,
		},
		{
			name: "Boolean to string false",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "false",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "string",
			},
			want:    "0",
			wantErr: false,
		},
		{
			name: "Invalid boolean string",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				// value is not a valid boolean string
				value:        "notabool",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "string",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Boolean to EncodeBool",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        true,
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "default",
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1}, // Expecting boolean encoding, replace as needed
			wantErr: false,
		},
		{
			name: "Int to string",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "123",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "string",
			},
			want:    "123",
			wantErr: false,
		},
		{
			name: "Invalid int string",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				// value is not a valid int string
				value:        "notanint",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "string",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Int to EncodeInt",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        int32(12),
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "default",
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Expecting int encoding, replace as needed
			wantErr: false,
		},
		{
			name: "Int min value",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        int32(math.MaxInt32),
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "default",
			},
			want:    []byte{0, 0, 0, 0, 127, 255, 255, 255},
			wantErr: false,
		},
		{
			name: "BigInt to EncodeBigInt",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        int32(12),
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "default",
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12},
			wantErr: false,
		},
		{
			name: "BigInt max value",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        int64(math.MaxInt64),
				pv:           primitive.ProtocolVersion4,
				cqlType:      "bigint",
				responseType: "default",
			},
			want:    []byte{127, 255, 255, 255, 255, 255, 255, 255},
			wantErr: false,
		},
		{
			name: "BigInt min value",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        int64(math.MinInt64),
				pv:           primitive.ProtocolVersion4,
				cqlType:      "bigint",
				responseType: "default",
			},
			want:    []byte{128, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name: "Int max",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        int32(math.MaxInt32),
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "default",
			},
			want:    []byte{0, 0, 0, 0, 127, 255, 255, 255},
			wantErr: false,
		},
		{
			name: "Unsupported cqlType",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "anything",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "unsupported",
				responseType: "default",
			},
			want:    "anything",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DataConversionInInsertionIfRequired(tt.args.value, tt.args.pv, tt.args.cqlType, tt.args.responseType)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataConversionInInsertionIfRequired() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataConversionInInsertionIfRequired() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessCollectionColumnsForPrepareQueries_ComplexMetaAndNonCollection(t *testing.T) {
	translator := &Translator{
		Logger:              zap.NewNop(),
		SchemaMappingConfig: GetSchemaMappingConfig(),
	}
	protocolV := primitive.ProtocolVersion4
	tableName := "non_primitive_table"
	keySpace := "test_keyspace"

	// --- Helper data ---
	textValueBytes, _ := proxycore.EncodeType(datatype.Varchar, protocolV, "testValue")
	textValue2Bytes, _ := proxycore.EncodeType(datatype.Varchar, protocolV, "testValue2")
	textValue3Bytes, _ := proxycore.EncodeType(datatype.Varchar, protocolV, "newValue")
	intValueBytes, _ := proxycore.EncodeType(datatype.Int, protocolV, int32(123))

	// Set data
	setTextType := datatype.NewSetType(datatype.Varchar)
	setValue := []string{"elem1", "elem2"}
	setValueBytes, _ := proxycore.EncodeType(setTextType, protocolV, setValue)

	// --- Test Cases ---
	tests := []struct {
		name            string
		columnsResponse []Column
		values          []*primitive.Value
		complexMeta     map[string]*ComplexOperation
		primaryKeys     []string
		// Expected outputs
		wantNewColumns   []Column
		wantNewValues    []interface{}
		wantUnencrypted  map[string]interface{}
		wantIndexEnd     int
		wantDelColFamily []string
		wantDelColumns   []Column
		wantErr          bool
	}{
		{
			name: "Non-collection column (text)",
			columnsResponse: []Column{
				{Name: "pk_1_text", CQLType: "varchar", ColumnFamily: "cf1"},
			},
			values: []*primitive.Value{
				{Contents: textValueBytes},
			},
			complexMeta: map[string]*ComplexOperation{},
			primaryKeys: []string{"pk_1_text"},
			wantNewColumns: []Column{
				{Name: "pk_1_text", CQLType: "varchar", ColumnFamily: "cf1"},
			},
			wantNewValues:    []interface{}{textValueBytes},
			wantUnencrypted:  map[string]interface{}{"pk_1_text": "testValue"},
			wantIndexEnd:     0,
			wantDelColFamily: nil,
			wantDelColumns:   nil,
			wantErr:          false,
		},
		{
			name: "Non-collection column (int)",
			columnsResponse: []Column{
				{Name: "column_int", CQLType: "int", ColumnFamily: "cf1"},
			},
			values: []*primitive.Value{
				{Contents: intValueBytes},
			},
			complexMeta: map[string]*ComplexOperation{},
			primaryKeys: []string{},
			wantNewColumns: []Column{
				{Name: "column_int", CQLType: "int", ColumnFamily: "cf1"},
			},

			wantNewValues:    []interface{}{[]byte{0, 0, 0, 0, 0, 0, 0, 123}},
			wantUnencrypted:  map[string]interface{}{},
			wantIndexEnd:     0,
			wantDelColFamily: nil,
			wantDelColumns:   nil,
			wantErr:          false,
		},
		{
			name: "Map append for specific key",
			columnsResponse: []Column{
				{Name: "map_text_text", CQLType: "map<varchar,varchar>", ColumnFamily: "map_text_text"},
			},
			values: []*primitive.Value{
				{Contents: textValue2Bytes},
			},
			complexMeta: map[string]*ComplexOperation{
				"map_text_text": {
					Append:           true,
					mapKey:           "newKey",
					ExpectedDatatype: datatype.Varchar,
				},
			},
			primaryKeys: []string{},
			wantNewColumns: []Column{
				{Name: "newKey", ColumnFamily: "map_text_text", CQLType: "varchar"},
			},
			wantNewValues:    []interface{}{textValue2Bytes},
			wantUnencrypted:  map[string]interface{}{},
			wantIndexEnd:     0,
			wantDelColFamily: nil,
			wantDelColumns:   nil,
			wantErr:          false,
		},
		{
			name: "Map delete",
			columnsResponse: []Column{
				{Name: "map_text_text", CQLType: "map<varchar,varchar>", ColumnFamily: "map_text_text"},
			},
			values: []*primitive.Value{
				// Value contains the keys to delete, encoded as a set<text>
				{Contents: setValueBytes},
			},
			complexMeta: map[string]*ComplexOperation{
				"map_text_text": {
					Delete:           true,
					ExpectedDatatype: setTextType, // Expecting a set of keys to delete
				},
			},
			primaryKeys:      []string{},
			wantNewColumns:   nil, // No new columns added
			wantNewValues:    []interface{}{},
			wantUnencrypted:  map[string]interface{}{},
			wantIndexEnd:     0,
			wantDelColFamily: nil, // Delete specific keys, not the whole family
			wantDelColumns: []Column{
				{Name: "elem1", ColumnFamily: "map_text_text"},
				{Name: "elem2", ColumnFamily: "map_text_text"},
			},
			wantErr: false,
		},
		{
			name: "List update by index",
			columnsResponse: []Column{
				{Name: "list_text", CQLType: "list<text>", ColumnFamily: "list_text"},
			},
			values: []*primitive.Value{
				{Contents: textValue3Bytes}, // The new value for the specific index
			},
			complexMeta: map[string]*ComplexOperation{
				"list_text": {
					UpdateListIndex: "1", // Update index 1
				},
			},
			primaryKeys:      []string{},
			wantNewColumns:   nil, // Update by index doesn't add new columns here, it modifies the meta
			wantNewValues:    []interface{}{},
			wantUnencrypted:  map[string]interface{}{},
			wantIndexEnd:     0,
			wantDelColFamily: nil,
			wantDelColumns:   nil,
			wantErr:          false,
			// We also need to check if complexMeta["list_text"].Value was updated, but that's harder in this structure
		},
		{
			name: "Set delete elements",
			columnsResponse: []Column{
				{Name: "set_text", CQLType: "set<text>", ColumnFamily: "set_text"},
			},
			values: []*primitive.Value{
				{Contents: setValueBytes}, // The set containing elements to delete
			},
			complexMeta: map[string]*ComplexOperation{
				"set_text": {
					Delete:           true,
					ExpectedDatatype: setTextType,
				},
			},
			primaryKeys:      []string{},
			wantNewColumns:   nil,
			wantNewValues:    []interface{}{},
			wantUnencrypted:  map[string]interface{}{},
			wantIndexEnd:     0,
			wantDelColFamily: nil, // Deleting specific elements
			wantDelColumns: []Column{
				{Name: "elem1", ColumnFamily: "set_text"},
				{Name: "elem2", ColumnFamily: "set_text"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of complexMeta for each test run
			currentComplexMeta := make(map[string]*ComplexOperation)
			for k, v := range tt.complexMeta {
				metaCopy := *v // Shallow copy is enough for this test structure
				currentComplexMeta[k] = &metaCopy
			}

			input := ProcessPrepareCollectionsInput{
				ColumnsResponse: tt.columnsResponse,
				Values:          tt.values,
				TableName:       tableName,
				ProtocolV:       protocolV,
				PrimaryKeys:     tt.primaryKeys,
				Translator:      translator,
				KeySpace:        keySpace,
				ComplexMeta:     currentComplexMeta,
			}
			output, err := processCollectionColumnsForPrepareQueries(input)

			if (err != nil) != tt.wantErr {
				t.Errorf("processCollectionColumnsForPrepareQueries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return // Don't check results if an error was expected
			}

			// Sort slices of columns before comparing for deterministic results
			sort.Slice(output.NewColumns, func(i, j int) bool { return output.NewColumns[i].Name < output.NewColumns[j].Name })
			sort.Slice(tt.wantNewColumns, func(i, j int) bool { return tt.wantNewColumns[i].Name < tt.wantNewColumns[j].Name })
			sort.Slice(output.DelColumns, func(i, j int) bool { return output.DelColumns[i].Name < output.DelColumns[j].Name })
			sort.Slice(tt.wantDelColumns, func(i, j int) bool { return tt.wantDelColumns[i].Name < tt.wantDelColumns[j].Name })
			sort.Strings(output.DelColumnFamily)
			sort.Strings(tt.wantDelColFamily)

			// For list types, don't compare Names directly, normalize them for comparison
			if strings.Contains(tt.name, "List") {
				// Normalize the Name fields for comparison
				for i := range output.NewColumns {
					output.NewColumns[i].Name = fmt.Sprintf("list_index_%d", i)
				}
				for i := range tt.wantNewColumns {
					tt.wantNewColumns[i].Name = fmt.Sprintf("list_index_%d", i)
				}
			}

			if !reflect.DeepEqual(output.NewColumns, tt.wantNewColumns) {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.NewColumns = %v, want %v", output.NewColumns, tt.wantNewColumns)
			}
			// Comparing slices of interfaces containing byte slices requires careful comparison
			if len(output.NewValues) != len(tt.wantNewValues) {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.NewValues length = %d, want %d", len(output.NewValues), len(tt.wantNewValues))
			} else {
				// Simple byte comparison for this test setup
				for i := range output.NewValues {
					gotBytes, okGot := output.NewValues[i].([]byte)
					wantBytes, okWant := tt.wantNewValues[i].([]byte)
					if !okGot || !okWant || !reflect.DeepEqual(gotBytes, wantBytes) {
						t.Errorf("processCollectionColumnsForPrepareQueries() output.NewValues[%d] = %v, want %v", i, output.NewValues[i], tt.wantNewValues[i])
					}
				}
			}
			if !reflect.DeepEqual(output.Unencrypted, tt.wantUnencrypted) {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.Unencrypted = %v, want %v", output.Unencrypted, tt.wantUnencrypted)
			}
			if output.IndexEnd != tt.wantIndexEnd {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.IndexEnd = %v, want %v", output.IndexEnd, tt.wantIndexEnd)
			}
			if !reflect.DeepEqual(output.DelColumnFamily, tt.wantDelColFamily) {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.DelColumnFamily = %v, want %v", output.DelColumnFamily, tt.wantDelColFamily)
			}
			if !reflect.DeepEqual(output.DelColumns, tt.wantDelColumns) {
				t.Errorf("processCollectionColumnsForPrepareQueries() output.DelColumns = %v, want %v", output.DelColumns, tt.wantDelColumns)
			}

			// Specific checks for complex meta modifications
			if tt.name == "List update by index" {
				meta, ok := currentComplexMeta["list_text"]
				if !ok || meta.UpdateListIndex != "1" || !reflect.DeepEqual(meta.Value, textValue3Bytes) {
					t.Errorf("List update by index: complexMeta not updated correctly. Got: %+v", meta)
				}
			}
			if tt.name == "List delete elements" {
				meta, ok := currentComplexMeta["list_text"]
				// Assuming listValueBytes corresponds to ["testValue", "testValue2"]
				expectedDeleteValues := [][]byte{textValueBytes, textValue2Bytes}
				if !ok || !meta.ListDelete || len(meta.ListDeleteValues) != len(expectedDeleteValues) {
					t.Errorf("List delete elements: complexMeta not updated correctly. Got: %+v", meta)
				} else {
					// Sort before comparing byte slices within the slice
					sort.Slice(meta.ListDeleteValues, func(i, j int) bool {
						return string(meta.ListDeleteValues[i]) < string(meta.ListDeleteValues[j])
					})
					sort.Slice(expectedDeleteValues, func(i, j int) bool {
						return string(expectedDeleteValues[i]) < string(expectedDeleteValues[j])
					})
					if !reflect.DeepEqual(meta.ListDeleteValues, expectedDeleteValues) {
						t.Errorf("List delete elements: ListDeleteValues mismatch. Got: %v, Want: %v", meta.ListDeleteValues, expectedDeleteValues)
					}
				}
			}
		})
	}
}
func TestExtractMapKey(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantMapName string
		wantMapKey  string
	}{
		{
			name:        "Valid map key with quotes",
			input:       "mymap['key1']",
			wantMapName: "mymap",
			wantMapKey:  "key1",
		},
		{
			name:        "Valid map key without quotes",
			input:       "mymap[key1]",
			wantMapName: "mymap",
			wantMapKey:  "key1",
		},
		{
			name:        "No map key",
			input:       "mymap",
			wantMapName: "mymap",
			wantMapKey:  "",
		},
		{
			name:        "Invalid format - missing closing bracket",
			input:       "mymap[key1",
			wantMapName: "mymap[key1",
			wantMapKey:  "",
		},
		{
			name:        "Invalid format - missing opening bracket",
			input:       "mymap]key1]",
			wantMapName: "mymap]key1]",
			wantMapKey:  "",
		},
		{
			name:        "Multiple quotes in key",
			input:       "mymap['key''1']",
			wantMapName: "mymap",
			wantMapKey:  "key1",
		},
		{
			name:        "Complex map name",
			input:       "my_complex_map[key1]",
			wantMapName: "my_complex_map",
			wantMapKey:  "key1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMapName, gotMapKey := ExtractMapKey(tt.input)
			if gotMapName != tt.wantMapName {
				t.Errorf("ExtractMapKey() gotMapName = %v, want %v", gotMapName, tt.wantMapName)
			}
			if gotMapKey != tt.wantMapKey {
				t.Errorf("ExtractMapKey() gotMapKey = %v, want %v", gotMapKey, tt.wantMapKey)
			}
		})
	}
}
func Test_processListCollectionRaw(t *testing.T) {
	tests := []struct {
		name            string
		val             string
		colFamily       string
		cqlType         string
		prependCol      []string
		complexMeta     map[string]*ComplexOperation
		wantColumns     []Column
		wantValues      []interface{}
		wantComplexMeta map[string]*ComplexOperation
		wantErr         bool
	}{
		{
			name:        "Simple list<varchar>",
			val:         "['value1', 'value2']",
			colFamily:   "list_text",
			cqlType:     "varchar",
			prependCol:  []string{},
			complexMeta: make(map[string]*ComplexOperation),
			wantColumns: []Column{
				{Name: time.Now().Format("20060102150405.999"), ColumnFamily: "list_text", CQLType: "varchar"},
				{Name: time.Now().Format("20060102150405.998"), ColumnFamily: "list_text", CQLType: "varchar"},
			},
			wantValues: []interface{}{
				[]byte("value1"),
				[]byte("value2"),
			},
			wantComplexMeta: make(map[string]*ComplexOperation),
			wantErr:         false,
		},
		{
			name:        "List with index operation",
			val:         "['0:newvalue']",
			colFamily:   "list_text",
			cqlType:     "varchar",
			prependCol:  []string{},
			complexMeta: make(map[string]*ComplexOperation),
			wantColumns: nil,
			wantValues:  []interface{}{},
			wantComplexMeta: map[string]*ComplexOperation{
				"list_text": {
					UpdateListIndex: "0",
					Value:           []byte("newvalue"),
				},
			},
			wantErr: false,
		},
		{
			name:       "List with delete operation",
			val:        "['value1', 'value2']",
			colFamily:  "list_text",
			cqlType:    "varchar",
			prependCol: []string{},
			complexMeta: map[string]*ComplexOperation{
				"list_text": {
					ListDelete: true,
				},
			},
			wantColumns: nil,
			wantValues:  []interface{}{},
			wantComplexMeta: map[string]*ComplexOperation{
				"list_text": {
					ListDelete:       true,
					ListDeleteValues: [][]byte{[]byte("value1"), []byte("value2")},
				},
			},
			wantErr: false,
		},
		{
			name:        "List with prepend operation",
			val:         "['value1', 'value2']",
			colFamily:   "list_text",
			cqlType:     "varchar",
			prependCol:  []string{"list_text"},
			complexMeta: make(map[string]*ComplexOperation),
			wantColumns: []Column{
				{Name: time.Now().Format("20060102150405.001"), ColumnFamily: "list_text", CQLType: "varchar"},
				{Name: time.Now().Format("20060102150405.002"), ColumnFamily: "list_text", CQLType: "varchar"},
			},
			wantValues: []interface{}{
				[]byte("value1"),
				[]byte("value2"),
			},
			wantComplexMeta: make(map[string]*ComplexOperation),
			wantErr:         false,
		},
		{
			name:            "Invalid list format",
			val:             "[\"?value1', 'value2']",
			colFamily:       "list_text",
			cqlType:         "varchar",
			prependCol:      []string{},
			complexMeta:     make(map[string]*ComplexOperation),
			wantColumns:     nil,
			wantValues:      nil,
			wantComplexMeta: make(map[string]*ComplexOperation),
			wantErr:         true,
		},
		{
			name:        "Invalid index operation format",
			val:         "['0value']", // Missing colon
			colFamily:   "list_text",
			cqlType:     "varchar",
			prependCol:  []string{},
			complexMeta: make(map[string]*ComplexOperation),
			wantColumns: []Column{
				{Name: time.Now().Format("20060102150405.999"), ColumnFamily: "list_text", CQLType: "varchar"},
			},
			wantValues: []interface{}{
				[]byte("0value"),
			},
			wantComplexMeta: make(map[string]*ComplexOperation),
			wantErr:         false,
		},
		{
			name:        "List<int>",
			val:         "['42', '123']",
			colFamily:   "list_int",
			cqlType:     "int",
			prependCol:  []string{},
			complexMeta: make(map[string]*ComplexOperation),
			wantColumns: []Column{
				{Name: time.Now().Format("20060102150405.999"), ColumnFamily: "list_int", CQLType: "int"},
				{Name: time.Now().Format("20060102150405.998"), ColumnFamily: "list_int", CQLType: "int"},
			},
			wantValues: []interface{}{
				[]byte{0, 0, 0, 0, 0, 0, 0, 42},
				[]byte{0, 0, 0, 0, 0, 0, 0, 123},
			},
			wantComplexMeta: make(map[string]*ComplexOperation),
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotColumns, gotValues, err := processListCollectionRaw(tt.val, tt.colFamily, tt.cqlType, tt.prependCol, tt.complexMeta)

			if (err != nil) != tt.wantErr {
				t.Errorf("processListCollectionRaw() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// For list types, normalize Names for comparison
			if len(gotColumns) > 0 {
				for i := range gotColumns {
					gotColumns[i].Name = fmt.Sprintf("list_index_%d", i)
				}
				for i := range tt.wantColumns {
					tt.wantColumns[i].Name = fmt.Sprintf("list_index_%d", i)
				}
			}

			// Compare columns
			if !reflect.DeepEqual(gotColumns, tt.wantColumns) {
				t.Errorf("processListCollectionRaw() gotColumns = %v, want %v", gotColumns, tt.wantColumns)
			}

			// Compare values length
			if len(gotValues) != len(tt.wantValues) {
				t.Errorf("processListCollectionRaw() gotValues length = %d, want %d", len(gotValues), len(tt.wantValues))
				return
			}

			// Compare values content
			for i := range gotValues {
				gotBytes, okGot := gotValues[i].([]byte)
				wantBytes, okWant := tt.wantValues[i].([]byte)
				if !okGot || !okWant {
					t.Errorf("processListCollectionRaw() value type mismatch at index %d", i)
					continue
				}
				if !reflect.DeepEqual(gotBytes, wantBytes) {
					t.Errorf("processListCollectionRaw() value mismatch at index %d: got %v, want %v", i, gotBytes, wantBytes)
				}
			}

			// Compare complexMeta
			if !reflect.DeepEqual(tt.complexMeta, tt.wantComplexMeta) {
				t.Errorf("processListCollectionRaw() complexMeta = %v, want %v", tt.complexMeta, tt.wantComplexMeta)
			}
		})
	}
}

func TestProcessListColumnGeneric(t *testing.T) {
	protocolV := primitive.ProtocolVersion4
	tests := []struct {
		name        string
		decodedVal  interface{}
		colFamily   string
		cqlType     string
		complexMeta *ComplexOperation
		wantColumns []Column
		wantValues  []interface{}
		wantErr     bool
	}{
		{
			name:        "Process list of strings",
			decodedVal:  []string{"value1", "value2"},
			colFamily:   "cf1",
			cqlType:     "varchar",
			complexMeta: nil,
			wantColumns: []Column{
				{ColumnFamily: "cf1", CQLType: "varchar"},
				{ColumnFamily: "cf1", CQLType: "varchar"},
			},
			wantValues: []interface{}{
				[]byte("value1"),
				[]byte("value2"),
			},
			wantErr: false,
		},
		{
			name:        "Process list of integers",
			decodedVal:  []int32{42, 123},
			colFamily:   "cf1",
			cqlType:     "int",
			complexMeta: nil,
			wantColumns: []Column{
				{ColumnFamily: "cf1", CQLType: "int"},
				{ColumnFamily: "cf1", CQLType: "int"},
			},
			wantValues: []interface{}{
				[]byte{0, 0, 0, 0, 0, 0, 0, 42},
				[]byte{0, 0, 0, 0, 0, 0, 0, 123},
			},
			wantErr: false,
		},
		{
			name:        "Process list with prepend operation",
			decodedVal:  []string{"value1", "value2"},
			colFamily:   "cf1",
			cqlType:     "varchar",
			complexMeta: &ComplexOperation{PrependList: true},
			wantColumns: []Column{
				{ColumnFamily: "cf1", CQLType: "varchar"},
				{ColumnFamily: "cf1", CQLType: "varchar"},
			},
			wantValues: []interface{}{
				[]byte("value1"),
				[]byte("value2"),
			},
			wantErr: false,
		},
		{
			name:        "Process list with delete operation",
			decodedVal:  []string{"value1", "value2"},
			colFamily:   "cf1",
			cqlType:     "varchar",
			complexMeta: &ComplexOperation{ListDelete: true},
			wantColumns: nil,
			wantValues:  []interface{}{},
			wantErr:     false,
		},
		{
			name:        "Invalid decoded value type",
			decodedVal:  "not a list",
			colFamily:   "cf1",
			cqlType:     "varchar",
			complexMeta: nil,
			wantColumns: nil,
			wantValues:  nil,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotColumns []Column
			var gotValues []interface{}

			var err error
			switch v := tt.decodedVal.(type) {
			case []string:
				err = processListColumnGeneric[string](v, protocolV, tt.colFamily, tt.cqlType, &gotValues, &gotColumns, tt.complexMeta)
			case []int32:
				err = processListColumnGeneric[int32](v, protocolV, tt.colFamily, tt.cqlType, &gotValues, &gotColumns, tt.complexMeta)
			default:
				err = processListColumnGeneric[string](v, protocolV, tt.colFamily, tt.cqlType, &gotValues, &gotColumns, tt.complexMeta)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("processListColumnGeneric() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Check list delete values are set correctly
			if tt.complexMeta != nil && tt.complexMeta.ListDelete {
				if len(tt.complexMeta.ListDeleteValues) != len(tt.decodedVal.([]string)) {
					t.Errorf("ListDeleteValues length = %d, want %d",
						len(tt.complexMeta.ListDeleteValues), len(tt.decodedVal.([]string)))
				}
				return
			}

			// Check columns length
			if len(gotColumns) != len(tt.wantColumns) {
				t.Errorf("got %d columns, want %d", len(gotColumns), len(tt.wantColumns))
				return
			}

			// Check values length
			if len(gotValues) != len(tt.wantValues) {
				t.Errorf("got %d values, want %d", len(gotValues), len(tt.wantValues))
				return
			}

			// Compare column properties (excluding Name which contains timestamp)
			for i := range gotColumns {
				if gotColumns[i].ColumnFamily != tt.wantColumns[i].ColumnFamily {
					t.Errorf("Column %d: got ColumnFamily %v, want %v",
						i, gotColumns[i].ColumnFamily, tt.wantColumns[i].ColumnFamily)
				}
				if gotColumns[i].CQLType != tt.wantColumns[i].CQLType {
					t.Errorf("Column %d: got CQLType %v, want %v",
						i, gotColumns[i].CQLType, tt.wantColumns[i].CQLType)
				}
			}

			// Compare values
			for i := range gotValues {
				gotBytes, ok1 := gotValues[i].([]byte)
				wantBytes, ok2 := tt.wantValues[i].([]byte)
				if !ok1 || !ok2 {
					t.Errorf("Value %d: type conversion error", i)
					continue
				}
				if !reflect.DeepEqual(gotBytes, wantBytes) {
					t.Errorf("Value %d: got %v, want %v", i, gotBytes, wantBytes)
				}
			}
		})
	}
}
func TestProcessComplexUpdate_SuccessfulCases(t *testing.T) {
	translator := &Translator{
		SchemaMappingConfig: &schemaMapping.SchemaMappingConfig{
			TablesMetaData: map[string]map[string]map[string]*schemaMapping.Column{
				"keyspace1": {
					"table1": {
						"map_col": {
							ColumnName:   "map_col",
							ColumnType:   "map<varchar,varchar>",
							IsCollection: true,
						},
						"list_col": {
							ColumnName:   "list_col",
							ColumnType:   "list<text>",
							IsCollection: true,
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name           string
		columns        []Column
		values         []interface{}
		tableName      string
		keyspaceName   string
		prependColumns []string
		wantMeta       map[string]*ComplexOperation
		wantErr        bool
	}{
		{
			name: "map append operation",
			columns: []Column{
				{Name: "map_col", CQLType: "map<varchar,varchar>"},
			},
			values:         []interface{}{"map_col+{key:?}"},
			tableName:      "table1",
			keyspaceName:   "keyspace1",
			prependColumns: []string{},
			wantMeta: map[string]*ComplexOperation{
				"map_col": {
					Append:           true,
					mapKey:           "key",
					ExpectedDatatype: datatype.Varchar,
				},
			},
			wantErr: false,
		},
		{
			name: "list prepend operation",
			columns: []Column{
				{Name: "list_col", CQLType: "list<text>"},
			},
			values:         []interface{}{"list_col+?"},
			tableName:      "table1",
			keyspaceName:   "keyspace1",
			prependColumns: []string{"list_col"},
			wantMeta: map[string]*ComplexOperation{
				"list_col": {
					PrependList: true,
					mapKey:      "",
				},
			},
			wantErr: false,
		},
		{
			name: "multiple operations",
			columns: []Column{
				{Name: "map_col", CQLType: "map<varchar,varchar>"},
				{Name: "list_col", CQLType: "list<text>"},
			},
			values: []interface{}{
				"map_col+{key:?}",
				"list_col+?",
			},
			tableName:      "table1",
			keyspaceName:   "keyspace1",
			prependColumns: []string{"list_col"},
			wantMeta: map[string]*ComplexOperation{
				"map_col": {
					Append:           true,
					mapKey:           "key",
					ExpectedDatatype: datatype.Varchar,
				},
				"list_col": {
					PrependList: true,
					mapKey:      "",
				},
			},
			wantErr: false,
		},
		{
			name: "non-collection column operation",
			columns: []Column{
				{Name: "normal_col", CQLType: "varchar"},
			},
			values:         []interface{}{"normal_col+value"},
			tableName:      "table1",
			keyspaceName:   "keyspace1",
			prependColumns: []string{},
			wantMeta:       map[string]*ComplexOperation{},
			wantErr:        false,
		},
		{
			name: "skip invalid value type",
			columns: []Column{
				{Name: "map_col", CQLType: "map<varchar,varchar>"},
			},
			values:         []interface{}{123}, // Not a string
			tableName:      "table1",
			keyspaceName:   "keyspace1",
			prependColumns: []string{},
			wantMeta:       map[string]*ComplexOperation{},
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMeta, err := translator.ProcessComplexUpdate(tt.columns, tt.values, tt.tableName, tt.keyspaceName, tt.prependColumns)

			if (err != nil) != tt.wantErr {
				t.Errorf("ProcessComplexUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Compare metadata using custom comparison
			if len(gotMeta) != len(tt.wantMeta) {
				t.Errorf("ProcessComplexUpdate() metadata length = %d, want %d", len(gotMeta), len(tt.wantMeta))
				return
			}
			for k, got := range gotMeta {
				want, exists := tt.wantMeta[k]
				if !exists {
					t.Errorf("ProcessComplexUpdate() unexpected key %s in result", k)
					continue
				}
				if !compareComplexOperation(got, want) {
					t.Errorf("ProcessComplexUpdate() metadata mismatch for key %s:\ngot  = %+v\nwant = %+v", k, got, want)
				}
			}
		})
	}
}
func TestProcessColumnUpdate(t *testing.T) {
	tests := []struct {
		name           string
		val            string
		column         Column
		prependColumns []string
		want           *ComplexOperation
		wantErr        bool
	}{
		{
			name: "map append operation",
			val:  "map_col+{key:?}",
			column: Column{
				Name:    "map_col",
				CQLType: "map<varchar,varchar>",
			},
			prependColumns: []string{},
			want: &ComplexOperation{
				Append:           true,
				mapKey:           "key",
				ExpectedDatatype: datatype.Varchar,
			},
			wantErr: false,
		},
		{
			name: "map delete operation",
			val:  "map_col-{key}",
			column: Column{
				Name:    "map_col",
				CQLType: "map<varchar,varchar>",
			},
			prependColumns: []string{},
			want: &ComplexOperation{
				Delete:           true,
				ExpectedDatatype: datatype.NewSetType(datatype.Varchar),
				mapKey:           "",
			},
			wantErr: false,
		},
		{
			name: "list prepend operation",
			val:  "list_col+?",
			column: Column{
				Name:    "list_col",
				CQLType: "list<text>",
			},
			prependColumns: []string{"list_col"},
			want: &ComplexOperation{
				PrependList: true,
				mapKey:      "",
			},
			wantErr: false,
		},
		{
			name: "list append operation",
			val:  "list_col+?",
			column: Column{
				Name:    "list_col",
				CQLType: "list<text>",
			},
			prependColumns: []string{},
			want: &ComplexOperation{
				Append: true,
				mapKey: "",
			},
			wantErr: false,
		},
		{
			name: "list delete operation",
			val:  "list_col-?",
			column: Column{
				Name:    "list_col",
				CQLType: "list<text>",
			},
			prependColumns: []string{},
			want: &ComplexOperation{
				Delete:     true,
				ListDelete: true,
				mapKey:     "",
			},
			wantErr: false,
		},
		{
			name: "list update by index",
			val:  "list_col+{1:?}",
			column: Column{
				Name:    "list_col",
				CQLType: "list<text>",
			},
			prependColumns: []string{},
			want: &ComplexOperation{
				Append:           false,
				mapKey:           "",
				UpdateListIndex:  "1",
				ExpectedDatatype: datatype.Varchar,
			},
			wantErr: false,
		},
		{
			name: "invalid format for map update",
			val:  "map_col+{invalid",
			column: Column{
				Name:    "map_col",
				CQLType: "map<varchar,varchar>",
			},
			prependColumns: []string{},
			want:           nil,
			wantErr:        true,
		},
		{
			name: "no operation specified",
			val:  "col_name",
			column: Column{
				Name:    "col_name",
				CQLType: "varchar",
			},
			prependColumns: []string{},
			want: &ComplexOperation{
				Append:      false,
				Delete:      false,
				ListDelete:  false,
				PrependList: false,
				mapKey:      "",
			},
			wantErr: false,
		},
		{
			name: "map with non-text key type",
			val:  "map_col+{42:?}",
			column: Column{
				Name:    "map_col",
				CQLType: "map<int,text>",
			},
			prependColumns: []string{},
			want: &ComplexOperation{
				Append:           true,
				mapKey:           "42",
				ExpectedDatatype: datatype.Varchar,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processColumnUpdate(tt.val, tt.column, tt.prependColumns)
			if (err != nil) != tt.wantErr {
				t.Errorf("processColumnUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if !compareComplexOperation(got, tt.want) {
				t.Errorf("processColumnUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}
