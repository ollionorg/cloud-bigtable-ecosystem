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
	"reflect"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
	"go.uber.org/zap"
)

func TestTranslator_TranslateUpdateQuerytoBigtable(t *testing.T) {
	type fields struct {
		Logger *zap.Logger
	}
	type args struct {
		query string
	}

	valueBlob := "0x0000000000000003"
	setBlob, err := formatValues(valueBlob, "blob", 4)
	if err != nil {
		t.Errorf("formatValues() error = %v", err)
	}

	valueTimestamp := "2024-08-12T12:34:56Z"
	setTimestamp, err := formatValues(valueTimestamp, "timestamp", 4)
	if err != nil {
		t.Errorf("formatValues() error = %v", err)
	}

	valueInt := "123"
	setInt, err := formatValues(valueInt, "int", 4)
	if err != nil {
		t.Errorf("formatValues() error = %v", err)
	}

	valueBigInt := "1234567890"
	setBigInt, err := formatValues(valueBigInt, "bigint", 4)
	if err != nil {
		t.Errorf("formatValues() error = %v", err)
	}

	setTrueBool, err := formatValues("true", "boolean", 4)
	if err != nil {
		t.Errorf("formatValues() error = %v", err)
	}

	setSetText := "{item1,item2}"

	valueMapTextBool := "{key1:true,key2:false}"

	// Define value1 and value2 as text for WHERE clause
	value1 := "testText"
	value2 := "pk_2_text_value"

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *UpdateQueryMapping
		wantErr bool
	}{
		{
			name: "update blob column",
			args: args{
				query: "UPDATE test_keyspace.test_table SET blob_col = '0x0000000000000003' WHERE pk_1_text = 'testText' AND pk_2_text = 'pk_2_text_value';",
			},
			wantErr: false,
			want: &UpdateQueryMapping{
				ParamKeys: []string{"set1", "value1", "value2"},
				Params: map[string]interface{}{
					"set1":   setBlob,
					"value1": value1,
					"value2": value2,
				},
				RowKey:   "testText#pk_2_text_value",
				Keyspace: "test_keyspace",
			},
		},
		{
			name: "update boolean column",
			args: args{
				query: "UPDATE test_keyspace.test_table SET bool_col = true WHERE pk_1_text = 'testText' AND pk_2_text = 'pk_2_text_value';",
			},
			wantErr: false,
			want: &UpdateQueryMapping{
				ParamKeys: []string{"set1", "value1", "value2"},
				Params: map[string]interface{}{
					"set1":   setTrueBool,
					"value1": value1,
					"value2": value2,
				},
				RowKey:   "testText#pk_2_text_value",
				Keyspace: "test_keyspace",
			},
		},
		{
			name: "update timestamp column",
			args: args{
				query: "UPDATE test_keyspace.test_table SET timestamp_col = '2024-08-12T12:34:56Z' WHERE pk_1_text = 'testText' AND pk_2_text = 'pk_2_text_value';",
			},
			wantErr: false,
			want: &UpdateQueryMapping{
				ParamKeys: []string{"set1", "value1", "value2"},
				Params: map[string]interface{}{
					"set1":   setTimestamp,
					"value1": value1,
					"value2": value2,
				},
				RowKey:   "testText#pk_2_text_value",
				Keyspace: "test_keyspace",
			},
		},
		{
			name: "update int column",
			args: args{
				query: "UPDATE test_keyspace.test_table SET int_col = 123 WHERE pk_1_text = 'testText' AND pk_2_text = 'pk_2_text_value';",
			},
			wantErr: false,
			want: &UpdateQueryMapping{
				ParamKeys: []string{"set1", "value1", "value2"},
				Params: map[string]interface{}{
					"set1":   setInt,
					"value1": value1,
					"value2": value2,
				},
				RowKey:   "testText#pk_2_text_value",
				Keyspace: "test_keyspace",
			},
		},
		{
			name: "update set<text> column",
			args: args{
				query: "UPDATE test_keyspace.test_table SET set_text_col = {'item1', 'item2'} WHERE pk_1_text = 'testText' AND pk_2_text = 'pk_2_text_value';",
			},
			wantErr: false,
			want: &UpdateQueryMapping{
				ParamKeys: []string{"set1", "value1", "value2"},
				Params: map[string]interface{}{
					"set1":   setSetText,
					"value1": value1,
					"value2": value2,
				},
				RowKey:   "testText#pk_2_text_value",
				Keyspace: "test_keyspace",
			},
		},
		{
			name: "update map<text,boolean> column",
			args: args{
				query: "UPDATE test_keyspace.test_table SET map_text_bool_col = {'key1': true, 'key2': false} WHERE pk_1_text = 'testText' AND pk_2_text = 'pk_2_text_value';",
			},
			wantErr: false,
			want: &UpdateQueryMapping{
				ParamKeys: []string{"set1", "value1", "value2"},
				Params: map[string]interface{}{
					"set1":   valueMapTextBool,
					"value1": value1,
					"value2": value2,
				},
				RowKey:   "testText#pk_2_text_value",
				Keyspace: "test_keyspace",
			},
		},
		{
			name: "update bigint column",
			args: args{
				query: "UPDATE test_keyspace.test_table SET bigint_col = 1234567890 WHERE pk_1_text = 'testText' AND pk_2_text = 'pk_2_text_value';",
			},
			wantErr: false,
			want: &UpdateQueryMapping{
				ParamKeys: []string{"set1", "value1", "value2"},
				Params: map[string]interface{}{
					"set1":   setBigInt,
					"value1": value1,
					"value2": value2,
				},
				RowKey:   "testText#pk_2_text_value",
				Keyspace: "test_keyspace",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaMapping := &schemaMapping.SchemaMappingConfig{
				Logger:          tt.fields.Logger,
				TablesMetaData:  mockSchemaMappingConfig,
				PkMetadataCache: mockPkMetadata,
			}

			tr := &Translator{
				Logger:              tt.fields.Logger,
				SchemaMappingConfig: schemaMapping,
			}
			got, err := tr.TranslateUpdateQuerytoBigtable(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.TranslateUpdateQuerytoBigtable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check specific fields instead of deep comparing the whole struct initially
			if got != nil {
				if !reflect.DeepEqual(got.Params, tt.want.Params) {
					t.Errorf("Translator.TranslateUpdateQuerytoBigtable() Params = %v, want %v", got.Params, tt.want.Params)
				}
				if !reflect.DeepEqual(got.ParamKeys, tt.want.ParamKeys) {
					t.Errorf("Translator.TranslateUpdateQuerytoBigtable() ParamKeys = %v, want %v", got.ParamKeys, tt.want.ParamKeys)
				}
				if !reflect.DeepEqual(got.RowKey, tt.want.RowKey) {
					t.Errorf("Translator.TranslateUpdateQuerytoBigtable() RowKey = %v, want %v", got.RowKey, tt.want.RowKey)
				}
				if !reflect.DeepEqual(got.Keyspace, tt.want.Keyspace) {
					t.Errorf("Translator.TranslateUpdateQuerytoBigtable() Keyspace = %v, want %v", got.Keyspace, tt.want.Keyspace)
				}
			} else if tt.want != nil {
				t.Errorf("Translator.TranslateUpdateQuerytoBigtable() got nil, want %v", tt.want)
			}
		})
	}
}

func TestTranslator_BuildUpdatePrepareQuery(t *testing.T) {
	type fields struct {
		Logger              *zap.Logger
		SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	}
	type args struct {
		columnsResponse []Column
		values          []*primitive.Value
		st              *UpdateQueryMapping
		protocolV       primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *UpdateQueryMapping
		wantErr bool
	}{
		{
			name: "Valid Input",
			fields: fields{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			args: args{
				values: []*primitive.Value{
					{Contents: []byte("")},
					{Contents: []byte("")},
				},
				columnsResponse: []Column{
					{
						Name:         "pk_1_text",
						ColumnFamily: "",
						CQLType:      "text",
					},
				},
				st: &UpdateQueryMapping{
					Query:       "Update blob_col=? FROM test_table where pk_1_text=?",
					QueryType:   "Update",
					Table:       "test_table",
					Keyspace:    "test_keyspace",
					PrimaryKeys: []string{"pk_1_text"},
					RowKey:      "pk_1_text_value", // Example RowKey based on pk_1_text
					Clauses: []Clause{
						{
							Column:       "pk_1_text",
							Operator:     "=",
							Value:        "",
							IsPrimaryKey: true,
						},
					},
					VariableMetadata: []*message.ColumnMetadata{
						{
							Name: "blob_col",
							Type: datatype.Blob,
						},
						{
							Name: "pk_1_text",
							Type: datatype.Varchar,
						},
					},
				},
			},
			want: &UpdateQueryMapping{
				Query:       "Update blob_col=? FROM test_table where pk_1_text=?",
				QueryType:   "Update",
				Keyspace:    "test_keyspace",
				PrimaryKeys: []string{"pk_1_text"},
				RowKey:      "",
				Table:       "test_table",
				Clauses: []Clause{
					{
						Column:       "pk_1_text",
						Operator:     "=",
						Value:        "",
						IsPrimaryKey: true,
					},
				},
				Columns: []Column{
					{
						Name:         "blob_col",
						ColumnFamily: "",
						CQLType:      "blob",
					},
				},
				Values: []interface{}{[]interface{}(nil)},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Translator{
				Logger:              tt.fields.Logger,
				SchemaMappingConfig: tt.fields.SchemaMappingConfig,
			}
			got, err := tr.BuildUpdatePrepareQuery(tt.args.columnsResponse, tt.args.values, tt.args.st, tt.args.protocolV)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.BuildUpdatePrepareQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Comparing specific fields as the whole struct comparison might fail due to dynamic parts
			if got != nil && tt.want != nil {
				if got.Query != tt.want.Query {
					t.Errorf("Translator.BuildUpdatePrepareQuery() Query = %v, want %v", got.Query, tt.want.Query)
				}
				if !reflect.DeepEqual(got.PrimaryKeys, tt.want.PrimaryKeys) {
					t.Errorf("Translator.BuildUpdatePrepareQuery() PrimaryKeys = %v, want %v", got.PrimaryKeys, tt.want.PrimaryKeys)
				}
				if len(got.Clauses) != len(tt.want.Clauses) {
					t.Errorf("Translator.BuildUpdatePrepareQuery() Clauses length mismatch = %d, want %d", len(got.Clauses), len(tt.want.Clauses))
				} else {
					for i := range got.Clauses {
						if !reflect.DeepEqual(got.Clauses[i], tt.want.Clauses[i]) {
							t.Errorf("Translator.BuildUpdatePrepareQuery() Clause[%d] = %v, want %v", i, got.Clauses[i], tt.want.Clauses[i])
						}
					}
				}
			} else if !(got == nil && tt.want == nil) {
				t.Errorf("Translator.BuildUpdatePrepareQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
