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

	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
	cql "github.com/ollionorg/cassandra-to-bigtable-proxy/translator/cqlparser"
	"go.uber.org/zap"
)

func parseInsertQuery() cql.IInsertContext {
	query := renameLiterals("INSERT INTO xobani_derived.user_info ( name ) VALUES ('Test')")
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	insertObj := p.Insert()
	insertObj.KwInto()
	return insertObj
}

func parseFailedInsertQuery() cql.IInsertContext {
	query := renameLiterals("INSERT INTO xobani_derived.user_info ( name ) VALUES")
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	insertObj := p.Insert()
	insertObj.KwInto()
	return insertObj
}

func Test_setParamsFromValues(t *testing.T) {
	var protocalV primitive.ProtocolVersion = 4
	insertObj := parseInsertQuery()
	response := make(map[string]interface{})
	val, _ := formatValues("Test", "text", protocalV)
	response["name"] = val
	var respValue []interface{}
	respValue = append(respValue, val)
	unencrypted := make(map[string]interface{})
	unencrypted["name"] = "Test"
	type args struct {
		input         cql.IInsertValuesSpecContext
		columns       []Column
		schemaMapping *schemaMapping.SchemaMappingConfig
		protocolV     primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		want1   []interface{}
		want2   map[string]interface{}
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				input: insertObj.InsertValuesSpec(),
				columns: []Column{
					{
						Name:         "name",
						ColumnFamily: "cf1",
						CQLType:      "text",
					},
				},
				schemaMapping: GetSchemaMappingConfig(),
				protocolV:     protocalV,
			},
			want:    response,
			want1:   respValue,
			want2:   unencrypted,
			wantErr: false,
		},
		{
			name: "failed",
			args: args{
				input: nil,
				columns: []Column{
					{
						Name:         "name",
						ColumnFamily: "cf1",
						CQLType:      "text",
					},
				},
				schemaMapping: GetSchemaMappingConfig(),
				protocolV:     protocalV,
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			wantErr: true,
		},
		{
			name: "failed",
			args: args{
				input: parseFailedInsertQuery().InsertValuesSpec(),
				columns: []Column{
					{
						Name:         "name",
						ColumnFamily: "cf1",
						CQLType:      "text",
					},
				},
				schemaMapping: GetSchemaMappingConfig(),
				protocolV:     protocalV,
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			wantErr: true,
		},
		{
			name: "failed",
			args: args{
				input:         parseFailedInsertQuery().InsertValuesSpec(),
				columns:       nil,
				schemaMapping: GetSchemaMappingConfig(),
				protocolV:     protocalV,
			},
			want:    nil,
			want1:   nil,
			want2:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, err := setParamsFromValues(tt.args.input, tt.args.columns, tt.args.schemaMapping, tt.args.protocolV, "user_info", "test_keyspace")
			if (err != nil) != tt.wantErr {
				t.Errorf("setParamsFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setParamsFromValues() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("setParamsFromValues() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("setParamsFromValues() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func TestTranslator_TranslateInsertQuerytoBigtable(t *testing.T) {
	var protocolV primitive.ProtocolVersion = 4
	type fields struct {
		Logger              *zap.Logger
		SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	}
	type args struct {
		queryStr  string
		protocolV primitive.ProtocolVersion
	}

	// Define values and format them
	textValue := "test-text"
	blobValue := "0x0000000000000003"
	booleanValue := "true"
	timestampValue := "2024-08-12T12:34:56Z"
	intValue := "123"
	bigIntValue := "1234567890"
	pk2TextValue := "pk_2_text_value"

	formattedText, _ := formatValues(textValue, "text", protocolV)
	formattedBlob, _ := formatValues(blobValue, "blob", protocolV)
	formattedBoolean, _ := formatValues(booleanValue, "boolean", protocolV)
	formattedTimestamp, _ := formatValues(timestampValue, "timestamp", protocolV)
	formattedInt, _ := formatValues(intValue, "int", protocolV)
	formattedBigInt, _ := formatValues(bigIntValue, "bigint", protocolV)
	formattedPk2Text, _ := formatValues(pk2TextValue, "text", protocolV)

	values := []interface{}{
		formattedText,
		formattedBlob,
		formattedBoolean,
		formattedTimestamp,
		formattedInt,
		formattedBigInt,
		formattedPk2Text,
	}

	response := map[string]interface{}{
		"pk_1_text":     formattedText,
		"blob_col":      formattedBlob,
		"bool_col":      formattedBoolean,
		"timestamp_col": formattedTimestamp,
		"int_col":       formattedInt,
		"bigint_col":    formattedBigInt,
		"pk_2_text":     formattedPk2Text,
	}

	query := "INSERT INTO test_keyspace.test_table (pk_1_text, blob_col, bool_col, timestamp_col, int_col, bigint_col, pk_2_text) VALUES ('" +
		textValue + "', '" + blobValue + "', " + booleanValue + ", '" + timestampValue + "', " + intValue + ", " + bigIntValue + ", " + pk2TextValue + ")"

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *InsertQueryMapping
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				queryStr:  query,
				protocolV: protocolV,
			},
			fields: fields{
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want: &InsertQueryMapping{
				Query:     query,
				QueryType: "INSERT",
				Table:     "test_table",
				Keyspace:  "test_keyspace",
				Columns: []Column{
					{Name: "pk_1_text", ColumnFamily: "cf1", CQLType: "text", IsPrimaryKey: true},
					{Name: "blob_col", ColumnFamily: "cf1", CQLType: "blob", IsPrimaryKey: false},
					{Name: "bool_col", ColumnFamily: "cf1", CQLType: "boolean", IsPrimaryKey: false},
					{Name: "timestamp_col", ColumnFamily: "cf1", CQLType: "timestamp", IsPrimaryKey: false},
					{Name: "int_col", ColumnFamily: "cf1", CQLType: "int", IsPrimaryKey: false},
					{Name: "bigint_col", ColumnFamily: "cf1", CQLType: "bigint", IsPrimaryKey: false},
					{Name: "pk_2_text", ColumnFamily: "cf1", CQLType: "text", IsPrimaryKey: true},
				},
				Values:      values,
				Params:      response,
				ParamKeys:   []string{"pk_1_text", "blob_col", "bool_col", "timestamp_col", "int_col", "bigint_col", "pk_2_text"},
				PrimaryKeys: []string{"pk_1_text", "pk_2_text"},
				RowKey:      "test-text#pk_2_text_value",
			},
			wantErr: false,
		},
		{
			name: "failed",
			args: args{
				queryStr:  "INSERT INTO test_keyspace.test_table",
				protocolV: protocolV,
			},
			fields: fields{
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "failed - missing values",
			args: args{
				queryStr:  "INSERT INTO test_keyspace.test_table (pk_1_text) VALUES ",
				protocolV: protocolV,
			},
			fields: fields{
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "failed",
			args: args{
				queryStr:  "INSERT INTO test_keyspace.test_table VALUES ('test')",
				protocolV: protocolV,
			},
			fields: fields{
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "failed because one primary column (pk_2_text) is not present",
			args: args{
				queryStr: "INSERT INTO test_keyspace.test_table (pk_1_text, blob_col, bool_col, timestamp_col, int_col, bigint_col) VALUES ('" +
					textValue + "', '" + blobValue + "', " + booleanValue + ", '" + timestampValue + "', " + intValue + ", " + bigIntValue + ")",
				protocolV: protocolV,
			},
			fields: fields{
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Translator{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: tt.fields.SchemaMappingConfig,
			}
			got, err := tr.TranslateInsertQuerytoBigtable(tt.args.queryStr, tt.args.protocolV)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.TranslateInsertQuerytoBigtable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && len(got.Params) > 0 && !reflect.DeepEqual(got.Params, tt.want.Params) {
				t.Errorf("Translator.TranslateInsertQuerytoBigtable() = %v, want %v", got.Params, tt.want.Params)
			}

			if got != nil && len(got.ParamKeys) > 0 && !reflect.DeepEqual(got.ParamKeys, tt.want.ParamKeys) {
				t.Errorf("Translator.TranslateInsertQuerytoBigtable() = %v, want %v", got.ParamKeys, tt.want.ParamKeys)
			}
			if got != nil && len(got.Values) > 0 && !reflect.DeepEqual(got.Values, tt.want.Values) {
				t.Errorf("Translator.TranslateInsertQuerytoBigtable() = %v, want %v", got.Values, tt.want.Values)
			}
			if got != nil && len(got.Columns) > 0 && !reflect.DeepEqual(got.Columns, tt.want.Columns) {
				t.Errorf("Translator.TranslateInsertQuerytoBigtable() = %v, want %v", got.Columns, tt.want.Columns)
			}
			if got != nil && len(got.RowKey) > 0 && !reflect.DeepEqual(got.RowKey, tt.want.RowKey) {
				t.Errorf("Translator.TranslateInsertQuerytoBigtable() = %v, want %v", got.RowKey, tt.want.RowKey)
			}

			if got != nil && !reflect.DeepEqual(got.Keyspace, tt.want.Keyspace) {
				t.Errorf("Translator.TranslateInsertQuerytoBigtable() = %v, want %v", got.Keyspace, tt.want.Keyspace)
			}
		})
	}
}

func TestTranslator_BuildInsertPrepareQuery(t *testing.T) {
	type fields struct {
		Logger              *zap.Logger
		SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	}
	type args struct {
		columnsResponse []Column
		values          []*primitive.Value
		st              *InsertQueryMapping
		protocolV       primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *InsertQueryMapping
		wantErr bool
	}{
		{
			name: "Valid Input",
			fields: fields{
				Logger:              zap.NewNop(),
				SchemaMappingConfig: GetSchemaMappingConfig(),
			},
			args: args{
				columnsResponse: []Column{
					{
						Name:         "pk_1_text",
						ColumnFamily: "cf1",
						CQLType:      "text",
						IsPrimaryKey: true,
					},
				},
				values: []*primitive.Value{
					{Contents: []byte("123")},
				},
				st: &InsertQueryMapping{
					Query:       "INSERT INTO test_keyspace.test_table(pk_1_text) VALUES (?)",
					QueryType:   "Insert",
					Table:       "test_table",
					Keyspace:    "test_keyspace",
					PrimaryKeys: []string{"pk_1_text"},
					RowKey:      "123",
					Columns: []Column{
						{
							Name:         "pk_1_text",
							ColumnFamily: "cf1",
							CQLType:      "text",
							IsPrimaryKey: true,
						},
					},
					Params: map[string]interface{}{
						"pk_1_text": &primitive.Value{Contents: []byte("123")},
					},
					ParamKeys:     []string{"pk_1_text"},
					IfNotExists:   false,
					TimestampInfo: TimestampInfo{},
					VariableMetadata: []*message.ColumnMetadata{
						{
							Name: "pk_1_text",
							Type: datatype.Varchar,
						},
					},
				},
				protocolV: 4,
			},
			want: &InsertQueryMapping{
				Query:       "INSERT INTO test_keyspace.test_table(pk_1_text) VALUES (?)",
				QueryType:   "Insert",
				Table:       "test_table",
				Keyspace:    "test_keyspace",
				PrimaryKeys: []string{"pk_1_text"},
				RowKey:      "123",
				Columns: []Column{
					{
						Name:         "pk_1_text",
						ColumnFamily: "cf1",
						CQLType:      "text",
						IsPrimaryKey: true,
					},
				},
				Values: []interface{}{
					&primitive.Value{Contents: []byte("123")},
				},
				Params: map[string]interface{}{
					"pk_1_text": &primitive.Value{Contents: []byte("123")},
				},
				ParamKeys:     []string{"pk_1_text"},
				IfNotExists:   false,
				TimestampInfo: TimestampInfo{},
				VariableMetadata: []*message.ColumnMetadata{
					{
						Name: "pk_1_text",
						Type: datatype.Varchar,
					},
				},
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
			got, err := tr.BuildInsertPrepareQuery(tt.args.columnsResponse, tt.args.values, tt.args.st, tt.args.protocolV)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.BuildInsertPrepareQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.RowKey != tt.want.RowKey {
				t.Errorf("Translator.BuildInsertPrepareQuery() RowKey = %v, want %v", got.RowKey, tt.want.RowKey)
			}
			if len(got.Values) != len(tt.want.Values) {
				t.Errorf("Translator.BuildInsertPrepareQuery() Values length mismatch: got %d, want %d", len(got.Values), len(tt.want.Values))
				return
			}
		})
	}
}
