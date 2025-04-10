package proxy

import (
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
)

func GetSchemaMappingConfig() *schemaMapping.SchemaMappingConfig {
	return &schemaMapping.SchemaMappingConfig{
		// Logger:          tt.fields.Logger,
		TablesMetaData:     mockSchemaMapping,
		PkMetadataCache:    mockPkMetadata,
		SystemColumnFamily: "cf1",
	}
}

var mockSchemaMapping = map[string]map[string]map[string]*schemaMapping.Column{
	"test_keyspace": {"test_table": {
		"column1": &schemaMapping.Column{
			ColumnName:   "column1",
			ColumnType:   "text",
			IsPrimaryKey: true,
			PkPrecedence: 1,
		},
		"column2": &schemaMapping.Column{
			ColumnName:   "column2",
			ColumnType:   "blob",
			IsPrimaryKey: false,
		},
		"column3": &schemaMapping.Column{
			ColumnName:   "column3",
			ColumnType:   "boolean",
			IsPrimaryKey: false,
		},
		"column4": &schemaMapping.Column{
			ColumnName:   "column4",
			ColumnType:   "list<text>",
			IsPrimaryKey: false,
		},
		"column5": &schemaMapping.Column{
			ColumnName:   "column5",
			ColumnType:   "timestamp",
			IsPrimaryKey: false,
		},
		"column6": &schemaMapping.Column{
			ColumnName:   "column6",
			ColumnType:   "int",
			IsPrimaryKey: false,
		},
		"column7": &schemaMapping.Column{
			ColumnName:   "column7",
			ColumnType:   "frozen<set<text>>",
			IsPrimaryKey: false,
			IsCollection: true,
		},
		"column8": &schemaMapping.Column{
			ColumnName:   "column8",
			ColumnType:   "map<text, boolean>",
			IsPrimaryKey: false,
			IsCollection: true,
		},
		"column9": &schemaMapping.Column{
			ColumnName:   "column9",
			ColumnType:   "bigint",
			IsPrimaryKey: false,
		},
		"column10": &schemaMapping.Column{
			ColumnName:   "column10",
			ColumnType:   "text",
			IsPrimaryKey: true,
			PkPrecedence: 2,
		},
	},
		"user_info": {
			"name": &schemaMapping.Column{
				ColumnName:   "name",
				ColumnType:   "text",
				IsPrimaryKey: true,
				PkPrecedence: 0,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Keyspace: "user_info",
					Table:    "user_info",
					Name:     "name",
					Index:    0,
					Type:     datatype.Varchar,
				},
			},
			"age": &schemaMapping.Column{
				ColumnName:   "age",
				ColumnType:   "text",
				IsPrimaryKey: false,
				PkPrecedence: 0,
				IsCollection: false,
				Metadata: message.ColumnMetadata{
					Keyspace: "user_info",
					Table:    "user_info",
					Name:     "age",
					Index:    1,
					Type:     datatype.Varchar,
				},
			},
		}},
}

var mockPkMetadata = map[string]map[string][]schemaMapping.Column{
	"test_keyspace": {"test_table": {
		{
			ColumnName:   "column1",
			CQLType:      "text",
			IsPrimaryKey: true,
			PkPrecedence: 1,
		},
		{
			ColumnName:   "column10",
			ColumnType:   "text",
			IsPrimaryKey: true,
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
		}},
}
