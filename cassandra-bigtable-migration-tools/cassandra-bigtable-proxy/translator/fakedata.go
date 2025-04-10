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
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
)

var mockSchemaMappingConfig = map[string]map[string]map[string]*schemaMapping.Column{
	"test_keyspace": {
		"test_table": {
			"column1": &schemaMapping.Column{
				ColumnName:   "column1",
				CQLType:      "varchar",
				ColumnType:   "varchar",
				IsPrimaryKey: true,
				PkPrecedence: 1,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "pk_1_text",
					Index: 0,
					Type:  datatype.Varchar,
				},
			},
			"column2": &schemaMapping.Column{
				ColumnName:   "column2",
				CQLType:      "blob",
				ColumnType:   "blob",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "column2",
					Index: 1,
					Type:  datatype.Blob,
				},
			},
			"column3": &schemaMapping.Column{
				ColumnName:   "column3",
				CQLType:      "boolean",
				ColumnType:   "boolean",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "column3",
					Index: 3,
					Type:  datatype.Boolean,
				},
			},
			"column5": &schemaMapping.Column{
				ColumnName:   "column5",
				CQLType:      "timestamp",
				ColumnType:   "timestamp",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "column5",
					Index: 4,
					Type:  datatype.Timestamp,
				},
			},
			"column6": &schemaMapping.Column{
				ColumnName:   "column6",
				CQLType:      "int",
				ColumnType:   "int",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "column6",
					Index: 5,
					Type:  datatype.Int,
				},
			},
			"column7": &schemaMapping.Column{
				ColumnName:   "column7",
				CQLType:      "set<varchar>",
				ColumnType:   "set<varchar>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "column7",
					Index: 6,
					Type:  datatype.NewSetType(datatype.Varchar),
				},
			},
			"column8": &schemaMapping.Column{
				ColumnName:   "column8",
				CQLType:      "map<varchar,boolean>",
				ColumnType:   "map<varchar,boolean>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "column8",
					Index: 7,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Boolean),
				},
			},
			"column9": &schemaMapping.Column{
				ColumnName:   "column9",
				CQLType:      "bigint",
				ColumnType:   "bigint",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "column9",
					Index: 8,
					Type:  datatype.Bigint,
				},
			},
			"column10": &schemaMapping.Column{
				ColumnName:   "column10",
				CQLType:      "varchar",
				ColumnType:   "varchar",
				IsPrimaryKey: true,
				PkPrecedence: 2,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "pk_2_text",
					Index: 1,
					Type:  datatype.Varchar,
				},
			},
			"blob_col": &schemaMapping.Column{
				ColumnName:   "blob_col",
				ColumnType:   "blob",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "blob_col",
					Index: 2,
					Type:  datatype.Blob,
				},
			},
			"bool_col": &schemaMapping.Column{
				ColumnName:   "bool_col",
				ColumnType:   "boolean",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "bool_col",
					Index: 3,
					Type:  datatype.Boolean,
				},
			},
			"timestamp_col": &schemaMapping.Column{
				ColumnName:   "timestamp_col",
				ColumnType:   "timestamp",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "timestamp_col",
					Index: 4,
					Type:  datatype.Timestamp,
				},
			},
			"int_col": &schemaMapping.Column{
				ColumnName:   "int_col",
				ColumnType:   "int",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "int_col",
					Index: 5,
					Type:  datatype.Int,
				},
			},
			"set_text_col": &schemaMapping.Column{
				ColumnName:   "set_text_col",
				ColumnType:   "set<varchar>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "set_text_col",
					Index: 6,
					Type:  datatype.NewSetType(datatype.Varchar),
				},
			},
			"map_text_bool_col": &schemaMapping.Column{
				ColumnName:   "map_text_bool_col",
				ColumnType:   "map<varchar,boolean>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "map_text_bool_col",
					Index: 7,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Boolean),
				},
			},
			"bigint_col": &schemaMapping.Column{
				ColumnName:   "bigint_col",
				ColumnType:   "bigint",
				IsPrimaryKey: false,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "bigint_col",
					Index: 8,
					Type:  datatype.Bigint,
				},
			},

			"map_text_text": &schemaMapping.Column{
				ColumnName:   "map_text_text",
				CQLType:      "map<text,text>",
				ColumnType:   "map<text,text>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_text",
					Index: 0,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Varchar),
				},
			},
			"list_text": &schemaMapping.Column{
				ColumnName:   "list_text",
				CQLType:      "list<text>",
				ColumnType:   "list<text>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_text",
					Index: 21,
					Type:  datatype.NewListType(datatype.Varchar),
				},
			},
		},
		"int_table": {
			"num": &schemaMapping.Column{
				ColumnName:   "num",
				CQLType:      "int",
				ColumnType:   "int",
				IsPrimaryKey: true,
				PkPrecedence: 1,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Table: "int_table",
					Name:  "num",
					Index: 0,
					Type:  datatype.Int,
				},
			},
			"big_num": &schemaMapping.Column{
				ColumnName:   "big_num",
				CQLType:      "bigint",
				ColumnType:   "bigint",
				IsPrimaryKey: true,
				PkPrecedence: 2,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Table: "int_table",
					Name:  "big_num",
					Index: 0,
					Type:  datatype.Bigint,
				},
			},
			"name": &schemaMapping.Column{
				ColumnName:   "name",
				CQLType:      "varchar",
				ColumnType:   "varchar",
				IsPrimaryKey: false,
				PkPrecedence: 1,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Table: "int_table",
					Name:  "name",
					Index: 0,
					Type:  datatype.Varchar,
				},
			},
		},
		"user_info": {
			"name": &schemaMapping.Column{
				ColumnName:   "name",
				CQLType:      "varchar",
				ColumnType:   "varchar",
				IsPrimaryKey: true,
				PkPrecedence: 1,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Table: "user_info",
					Name:  "name",
					Index: 0,
					Type:  datatype.Varchar,
				},
			},
			"age": &schemaMapping.Column{
				ColumnName:   "age",
				CQLType:      "int",
				ColumnType:   "int",
				IsPrimaryKey: true,
				PkPrecedence: 2,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Table: "user_info",
					Name:  "age",
					Index: 1,
					Type:  datatype.Int,
				},
			},
		},
		"non_primitive_table": {
			"map_text_text": &schemaMapping.Column{
				ColumnName:   "map_text_text",
				CQLType:      "map<text,text>",
				ColumnType:   "map<text,text>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_text",
					Index: 0,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Varchar),
				},
			},
			"map_text_int": &schemaMapping.Column{
				ColumnName:   "map_text_int",
				CQLType:      "map<text,int>",
				ColumnType:   "map<text,int>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_int",
					Index: 1,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Int),
				},
			},
			"map_text_float": &schemaMapping.Column{
				ColumnName:   "map_text_float",
				CQLType:      "map<text,float>",
				ColumnType:   "map<text,float>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_float",
					Index: 2,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Float),
				},
			},
			"map_text_double": &schemaMapping.Column{
				ColumnName:   "map_text_double",
				CQLType:      "map<text,double>",
				ColumnType:   "map<text,double>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_double",
					Index: 3,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Double),
				},
			},
			"map_text_timestamp": &schemaMapping.Column{
				ColumnName:   "map_text_timestamp",
				CQLType:      "map<text,timestamp>",
				ColumnType:   "map<text,timestamp>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_timestamp",
					Index: 4,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Bigint),
				},
			},
			"map_timestamp_text": &schemaMapping.Column{
				ColumnName:   "map_timestamp_text",
				CQLType:      "map<timestamp,text>",
				ColumnType:   "map<timestamp,text>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_timestamp_text",
					Index: 5,
					Type:  datatype.NewMapType(datatype.Timestamp, datatype.Varchar),
				},
			},
			"map_timestamp_int": &schemaMapping.Column{
				ColumnName:   "map_timestamp_int",
				CQLType:      "map<timestamp,int>",
				ColumnType:   "map<timestamp,int>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_timestamp_int",
					Index: 6,
					Type:  datatype.NewMapType(datatype.Timestamp, datatype.Int),
				},
			},
			"map_timestamp_boolean": &schemaMapping.Column{
				ColumnName:   "map_timestamp_boolean",
				CQLType:      "map<timestamp,boolean>",
				ColumnType:   "map<timestamp,boolean>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_timestamp_boolean",
					Index: 7,
					Type:  datatype.NewMapType(datatype.Timestamp, datatype.Boolean),
				},
			},
			"map_timestamp_timestamp": &schemaMapping.Column{
				ColumnName:   "map_timestamp_timestamp",
				CQLType:      "map<timestamp,timestamp>",
				ColumnType:   "map<timestamp,timestamp>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_timestamp_timestamp",
					Index: 8,
					Type:  datatype.NewMapType(datatype.Timestamp, datatype.Timestamp),
				},
			},
			"map_timestamp_bigint": &schemaMapping.Column{
				ColumnName:   "map_timestamp_bigint",
				CQLType:      "map<timestamp,bigint>",
				ColumnType:   "map<timestamp,bigint>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_timestamp_bigint",
					Index: 9,
					Type:  datatype.NewMapType(datatype.Timestamp, datatype.Bigint),
				},
			},
			"map_timestamp_float": &schemaMapping.Column{
				ColumnName:   "map_timestamp_float",
				CQLType:      "map<timestamp,float>",
				ColumnType:   "map<timestamp,float>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_timestamp_float",
					Index: 10,
					Type:  datatype.NewMapType(datatype.Timestamp, datatype.Float),
				},
			},
			"map_timestamp_double": &schemaMapping.Column{
				ColumnName:   "map_timestamp_double",
				CQLType:      "map<timestamp,double>",
				ColumnType:   "map<timestamp,double>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_timestamp_double",
					Index: 11,
					Type:  datatype.NewMapType(datatype.Timestamp, datatype.Double),
				},
			},
			"set_text": &schemaMapping.Column{
				ColumnName:   "set_text",
				CQLType:      "set<varchar>",
				ColumnType:   "set<varchar>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "set_text",
					Index: 12,
					Type:  datatype.NewSetType(datatype.Varchar),
				},
			},
			"set_boolean": &schemaMapping.Column{
				ColumnName:   "set_boolean",
				CQLType:      "set<boolean>",
				ColumnType:   "set<boolean>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "set_boolean",
					Index: 13,
					Type:  datatype.NewSetType(datatype.Boolean),
				},
			},
			"set_int": &schemaMapping.Column{
				ColumnName:   "set_int",
				CQLType:      "set<set_int>",
				ColumnType:   "set<set_int>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "set_int",
					Index: 14,
					Type:  datatype.NewSetType(datatype.Int),
				},
			},
			"set_float": &schemaMapping.Column{
				ColumnName:   "set_float",
				CQLType:      "set<float>",
				ColumnType:   "set<float>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "set_float",
					Index: 15,
					Type:  datatype.NewSetType(datatype.Float),
				},
			},
			"set_double": &schemaMapping.Column{
				ColumnName:   "set_double",
				CQLType:      "set<double>",
				ColumnType:   "set<double>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "set_double",
					Index: 16,
					Type:  datatype.NewSetType(datatype.Double),
				},
			},
			"set_bigint": &schemaMapping.Column{
				ColumnName:   "set_bigint",
				CQLType:      "set<bigint>",
				ColumnType:   "set<bigint>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "set_bigint",
					Index: 17,
					Type:  datatype.NewSetType(datatype.Bigint),
				},
			},
			"set_timestamp": &schemaMapping.Column{
				ColumnName:   "set_timestamp",
				CQLType:      "set<timestamp>",
				ColumnType:   "set<timestamp>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "set_timestamp",
					Index: 18,
					Type:  datatype.NewSetType(datatype.Timestamp),
				},
			},
			"map_text_boolean": &schemaMapping.Column{
				ColumnName:   "map_text_boolean",
				CQLType:      "map<varchar,boolean>",
				ColumnType:   "map<varchar,boolean>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_boolean",
					Index: 19,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Boolean),
				},
			},
			"map_text_bigint": &schemaMapping.Column{
				ColumnName:   "map_text_bigint",
				CQLType:      "map<text,bigint>",
				ColumnType:   "map<text,bigint>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "map_text_bigint",
					Index: 20,
					Type:  datatype.NewMapType(datatype.Varchar, datatype.Bigint),
				},
			},
			"list_text": &schemaMapping.Column{
				ColumnName:   "list_text",
				CQLType:      "list<text>",
				ColumnType:   "list<text>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_text",
					Index: 21,
					Type:  datatype.NewListType(datatype.Varchar),
				},
			},
			"list_int": &schemaMapping.Column{
				ColumnName:   "list_int",
				CQLType:      "list<int>",
				ColumnType:   "list<int>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_int",
					Index: 22,
					Type:  datatype.NewListType(datatype.Int),
				},
			},
			"list_float": &schemaMapping.Column{
				ColumnName:   "list_float",
				CQLType:      "list<float>",
				ColumnType:   "list<float>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_float",
					Index: 23,
					Type:  datatype.NewListType(datatype.Float),
				},
			},
			"list_double": &schemaMapping.Column{
				ColumnName:   "list_double",
				CQLType:      "list<double>",
				ColumnType:   "list<double>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_double",
					Index: 24,
					Type:  datatype.NewListType(datatype.Double),
				},
			},
			"list_boolean": &schemaMapping.Column{
				ColumnName:   "list_boolean",
				CQLType:      "list<boolean>",
				ColumnType:   "list<boolean>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_boolean",
					Index: 25,
					Type:  datatype.NewListType(datatype.Boolean),
				},
			},
			"list_timestamp": &schemaMapping.Column{
				ColumnName:   "list_timestamp",
				CQLType:      "list<timestamp>",
				ColumnType:   "list<timestamp>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_timestamp",
					Index: 26,
					Type:  datatype.NewListType(datatype.Timestamp),
				},
			},
			"list_bigint": &schemaMapping.Column{
				ColumnName:   "list_bigint",
				ColumnType:   "list<bigint>",
				IsPrimaryKey: false,
				IsCollection: true,
				Metadata: message.ColumnMetadata{
					Table: "non_primitive_table",
					Name:  "list_bigint",
					Index: 27,
					Type:  datatype.NewListType(datatype.Bigint),
				},
			},
		},
	},
}

var mockPkMetadata = map[string]map[string][]schemaMapping.Column{
	"test_keyspace": {"test_table": {
		{
			ColumnName:   "column1",
			CQLType:      "varchar",
			ColumnType:   "varchar",
			IsPrimaryKey: true,
			PkPrecedence: 1,
			Metadata: message.ColumnMetadata{
				Table: "test_table",
				Name:  "column1",
				Index: 0,
				Type:  datatype.Varchar,
			},
		},
		{
			ColumnName:   "column10",
			CQLType:      "varchar",
			ColumnType:   "varchar",
			IsPrimaryKey: true,
			PkPrecedence: 2,
			Metadata: message.ColumnMetadata{
				Table: "test_table",
				Name:  "column10",
				Index: 9,
				Type:  datatype.Varchar,
			},
		},
	},
		"int_table": {
			{
				ColumnName:   "num",
				CQLType:      "int",
				ColumnType:   "int",
				IsPrimaryKey: true,
				PkPrecedence: 1,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "num",
					Index: 0,
					Type:  datatype.Int,
				},
			},
			{
				ColumnName:   "big_num",
				CQLType:      "bigint",
				ColumnType:   "bigint",
				IsPrimaryKey: true,
				PkPrecedence: 2,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "big_num",
					Index: 9,
					Type:  datatype.Bigint,
				},
			},
		},
		"user_info": {
			{
				ColumnName:   "name",
				CQLType:      "varchar",
				ColumnType:   "varchar",
				IsPrimaryKey: true,
				PkPrecedence: 1,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Table: "user_info",
					Name:  "name",
					Index: 0,
					Type:  datatype.Varchar,
				},
			},
			{
				ColumnName:   "age",
				CQLType:      "int",
				ColumnType:   "int",
				IsPrimaryKey: true,
				PkPrecedence: 2,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Table: "user_info",
					Name:  "age",
					Index: 1,
					Type:  datatype.Int,
				},
			},
		}},
}

func GetSchemaMappingConfig() *schemaMapping.SchemaMappingConfig {
	return &schemaMapping.SchemaMappingConfig{
		TablesMetaData:     mockSchemaMappingConfig,
		PkMetadataCache:    mockPkMetadata,
		SystemColumnFamily: "cf1",
	}
}
