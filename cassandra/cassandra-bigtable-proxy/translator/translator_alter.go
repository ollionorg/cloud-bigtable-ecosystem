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

	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/message"
	cql "github.com/ollionorg/cassandra-to-bigtable-proxy/third_party/cqlparser"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/utilities"
)

func (t *Translator) TranslateAlterTableToBigtable(query string) (*AlterTableStatementMap, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	alterTable := p.AlterTable()

	if alterTable == nil {
		return nil, errors.New("error while parsing alter statement")
	}

	table := alterTable.Table().GetText()
	if alterTable.Keyspace() == nil {
		return nil, errors.New("missing keyspace. keyspace is required")
	}
	keyspace := alterTable.Keyspace().GetText()

	var dropColumns []string
	if alterTable.AlterTableOperation().AlterTableDropColumns() != nil {
		for _, dropColumn := range alterTable.AlterTableOperation().AlterTableDropColumns().AlterTableDropColumnList().AllColumn() {
			dropColumns = append(dropColumns, dropColumn.GetText())
		}
	}
	var addColumns []message.ColumnMetadata
	if alterTable.AlterTableOperation().AlterTableAdd() != nil {
		for i, addColumn := range alterTable.AlterTableOperation().AlterTableAdd().AlterTableColumnDefinition().AllColumn() {
			dt, err := utilities.GetCassandraColumnType(alterTable.AlterTableOperation().AlterTableAdd().AlterTableColumnDefinition().DataType(i).GetText())
			if err != nil {
				return nil, err
			}

			addColumns = append(addColumns, message.ColumnMetadata{
				Table:    table,
				Keyspace: keyspace,
				Type:     dt,
				Name:     addColumn.GetText(),
				Index:    int32(i),
			})
		}
	}

	if alterTable.AlterTableOperation().AlterTableRename() != nil && len(alterTable.AlterTableOperation().AlterTableRename().AllColumn()) != 0 {
		return nil, errors.New("rename operation in alter table command not supported")
	}

	var stmt = AlterTableStatementMap{
		Table:       table,
		Keyspace:    keyspace,
		QueryType:   "alter",
		DropColumns: dropColumns,
		AddColumns:  addColumns,
	}

	return &stmt, nil
}
