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

func (t *Translator) TranslateCreateTableToBigtable(query string) (*CreateTableStatementMap, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	createTableObj := p.CreateTable()

	if createTableObj == nil {
		return nil, errors.New("error while parsing create object")
	}

	table := createTableObj.Table().GetText()
	if createTableObj.Keyspace() == nil {
		return nil, errors.New("missing keyspace. keyspace is required")
	}
	keyspace := createTableObj.Keyspace().GetText()

	var pmks []CreateTablePrimaryKeyConfig
	var columns []message.ColumnMetadata
	for i, col := range createTableObj.ColumnDefinitionList().AllColumnDefinition() {
		dt, err := utilities.GetCassandraColumnType(col.DataType().GetText())

		if err != nil {
			return nil, err
		}

		columns = append(columns, message.ColumnMetadata{
			Table:    table,
			Keyspace: keyspace,
			Type:     dt,
			Name:     col.Column().GetText(),
			Index:    int32(i),
		})

		if col.PrimaryKeyColumn() != nil {
			pmks = append(pmks, CreateTablePrimaryKeyConfig{
				Name:    col.Column().GetText(),
				KeyType: utilities.KEY_TYPE_REGULAR,
			})
		}
	}

	// nil if inline primary key definition used
	if createTableObj.ColumnDefinitionList().PrimaryKeyElement() != nil {
		singleKey := createTableObj.ColumnDefinitionList().PrimaryKeyElement().PrimaryKeyDefinition().SinglePrimaryKey()
		compoundKey := createTableObj.ColumnDefinitionList().PrimaryKeyElement().PrimaryKeyDefinition().CompoundKey()
		compositeKey := createTableObj.ColumnDefinitionList().PrimaryKeyElement().PrimaryKeyDefinition().CompositeKey()
		if singleKey != nil {
			pmks = []CreateTablePrimaryKeyConfig{
				{
					Name:    singleKey.GetText(),
					KeyType: utilities.KEY_TYPE_PARTITION,
				},
			}
		} else if compoundKey != nil {
			pmks = append(pmks, CreateTablePrimaryKeyConfig{
				Name:    compoundKey.PartitionKey().GetText(),
				KeyType: utilities.KEY_TYPE_PARTITION,
			})
			for _, clusterKey := range compoundKey.ClusteringKeyList().AllClusteringKey() {
				pmks = append(pmks, CreateTablePrimaryKeyConfig{
					Name:    clusterKey.Column().GetText(),
					KeyType: utilities.KEY_TYPE_CLUSTERING,
				})
			}
		} else if compositeKey != nil {
			for _, partitionKey := range compositeKey.PartitionKeyList().AllPartitionKey() {
				pmks = append(pmks, CreateTablePrimaryKeyConfig{
					Name:    partitionKey.Column().GetText(),
					KeyType: utilities.KEY_TYPE_PARTITION,
				})
			}
			for _, clusterKey := range compositeKey.ClusteringKeyList().AllClusteringKey() {
				pmks = append(pmks, CreateTablePrimaryKeyConfig{
					Name:    clusterKey.Column().GetText(),
					KeyType: utilities.KEY_TYPE_CLUSTERING,
				})
			}
		} else {
			// this should never happen
			return nil, errors.New("unknown key type for table")
		}
	}

	var stmt = CreateTableStatementMap{
		Table:       table,
		IfNotExists: createTableObj.IfNotExist() != nil,
		Keyspace:    keyspace,
		QueryType:   "create",
		Columns:     columns,
		PrimaryKeys: pmks,
	}

	return &stmt, nil
}
