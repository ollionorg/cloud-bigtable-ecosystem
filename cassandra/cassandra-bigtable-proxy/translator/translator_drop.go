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
	cql "github.com/ollionorg/cassandra-to-bigtable-proxy/third_party/cqlparser"
)

func (t *Translator) TranslateDropTableToBigtable(query string) (*DropTableStatementMap, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	dropTableObj := p.DropTable()

	if dropTableObj == nil {
		return nil, errors.New("error while parsing drop table object")
	}

	table := dropTableObj.Table().GetText()
	if dropTableObj.Keyspace() == nil {
		return nil, errors.New("missing keyspace. keyspace is required")
	}
	keyspace := dropTableObj.Keyspace().GetText()

	var stmt = DropTableStatementMap{
		Table:     table,
		IfExists:  dropTableObj.IfExist() != nil,
		Keyspace:  keyspace,
		QueryType: "drop",
	}

	return &stmt, nil
}
