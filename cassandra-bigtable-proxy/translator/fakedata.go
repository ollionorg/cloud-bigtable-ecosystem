package translator

import (
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
)

var mockSchemaMappingConfig = map[string]map[string]map[string]*schemaMapping.Column{
	"test_keyspace": {
		"test_table": {
			"pk_1_text": &schemaMapping.Column{
				ColumnName:   "pk_1_text",
				ColumnType:   "text",
				IsPrimaryKey: true,
				PkPrecedence: 1,
				Metadata: message.ColumnMetadata{
					Table: "test_table",
					Name:  "pk_1_text",
					Index: 0,
					Type:  datatype.Varchar,
				},
			},
			"pk_2_text": &schemaMapping.Column{
				ColumnName:   "pk_2_text",
				ColumnType:   "text",
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
				ColumnType:   "set<text>",
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
				ColumnType:   "map<text,boolean>",
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
		"user_info": {
			"name": &schemaMapping.Column{
				ColumnName:   "name",
				ColumnType:   "text",
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
				ColumnType:   "set<text>",
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
				ColumnType:   "map<text,boolean>",
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
			ColumnName:   "pk_1_text",
			CQLType:      "text",
			IsPrimaryKey: true,
			PkPrecedence: 1,
			Metadata: message.ColumnMetadata{
				Table: "test_table",
				Name:  "pk_1_text",
				Index: 0,
				Type:  datatype.Varchar,
			},
		},
		{
			ColumnName:   "pk_2_text",
			ColumnType:   "text",
			IsPrimaryKey: true,
			PkPrecedence: 2,
			Metadata: message.ColumnMetadata{
				Table: "test_table",
				Name:  "pk_2_text",
				Index: 9,
				Type:  datatype.Varchar,
			},
		},
	},
		"user_info": {
			{
				ColumnName:   "name",
				ColumnType:   "text",
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
