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
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/assert"
)

func TestTranslateAlterTableToBigtable(t *testing.T) {
	var tests = []struct {
		query    string
		want     *AlterTableStatementMap
		hasError bool
	}{
		// add column
		{
			query: "ALTER TABLE my_keyspace.my_table ADD firstname text",
			want: &AlterTableStatementMap{
				Table:     "my_table",
				Keyspace:  "my_keyspace",
				QueryType: "alter",
				AddColumns: []message.ColumnMetadata{
					{
						Keyspace: "my_keyspace",
						Table:    "my_table",
						Name:     "firstname",
						Index:    0,
						Type:     datatype.Varchar,
					},
				},
			},
			hasError: false,
		},
		// add columns
		{
			query: "ALTER TABLE my_keyspace.my_table ADD firstname text, age int",
			want: &AlterTableStatementMap{
				Table:     "my_table",
				Keyspace:  "my_keyspace",
				QueryType: "alter",
				AddColumns: []message.ColumnMetadata{
					{
						Keyspace: "my_keyspace",
						Table:    "my_table",
						Name:     "firstname",
						Index:    0,
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "my_keyspace",
						Table:    "my_table",
						Name:     "age",
						Index:    1,
						Type:     datatype.Int,
					},
				},
			},
			hasError: false,
		},
		// drop column
		{
			query: "ALTER TABLE my_keyspace.my_table DROP firstname",
			want: &AlterTableStatementMap{
				Table:       "my_table",
				Keyspace:    "my_keyspace",
				QueryType:   "alter",
				DropColumns: []string{"firstname"},
			},
			hasError: false,
		},
		// drop columns
		{
			query: "ALTER TABLE my_keyspace.my_table DROP firstname, lastname",
			want: &AlterTableStatementMap{
				Table:       "my_table",
				Keyspace:    "my_keyspace",
				QueryType:   "alter",
				DropColumns: []string{"firstname", "lastname"},
			},
			hasError: false,
		},
		// rename not supported
		{
			query:    "ALTER TABLE my_keyspace.my_table RENAME col1 TO col2",
			want:     nil,
			hasError: true,
		},
	}

	tr := &Translator{
		Logger:              nil,
		SchemaMappingConfig: nil,
	}

	for _, tt := range tests {
		got, err := tr.TranslateAlterTableToBigtable(tt.query)
		if tt.hasError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		assert.True(t, (err != nil) == tt.hasError, tt.hasError)
		assert.Equal(t, tt.want, got)
	}
}
