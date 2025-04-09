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
	"fmt"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
	cql "github.com/ollionorg/cassandra-to-bigtable-proxy/third_party/cqlparser"
)

const (
	customWriteTime = "writetime_"
)

// parseColumnsFromSelect() parse Columns from the Select Query
//
// Parameters:
//   - input: The Select Element context from the antlr Parser.
//
// Returns: Column Meta and an error if any.
func parseColumnsFromSelect(input cql.ISelectElementsContext) (ColumnMeta, error) {
	var response ColumnMeta
	var funcName, argument string
	if input == nil {
		// todo - parser support for writetime - parser fails to get the selectItemList if first Item is writetime function
		return response, errors.New("no Input parameters found for columns")
	}

	if input.STAR() != nil {
		response.Star = true
	} else {
		columns := input.AllSelectElement()
		if len(columns) == 0 {
			// Todo - Code is not reachable
			return response, errors.New("no column parameters found in the query")
		}
		for _, val := range columns {
			var selectedColumns schemaMapping.SelectedColumns
			if val == nil {
				return response, errors.New("error while parsing the values")
			}
			funcCall := val.FunctionCall()
			if funcCall == nil {
				selectedColumns.Name = val.GetText()
				// Handle map access
				mapAccess := val.MapAccess()

				if mapAccess != nil {
					objectName := mapAccess.OBJECT_NAME(0).GetText()
					if mapAccess.KwAs() != nil && len(mapAccess.AllOBJECT_NAME()) > 1 {
						selectedColumns.IsAs = true
						selectedColumns.Alias = mapAccess.OBJECT_NAME(1).GetText()
					}
					mapKey := mapAccess.Constant().GetText()

					// Remove surrounding quotes from mapKey
					mapKey = strings.Trim(mapKey, "'")

					// Populate the selected column
					selectedColumns.Name = objectName + "_" + mapKey
					selectedColumns.MapColumnName = objectName
					selectedColumns.MapKey = mapKey

					// Append the parsed column to the response
					response.Column = append(response.Column, selectedColumns)
					continue
				}

			} else {
				selectedColumns.IsFunc = true
				if funcCall.OBJECT_NAME() == nil {
					return response, errors.New("function call object is nil")
				}
				funcName = strings.ToLower(funcCall.OBJECT_NAME().GetText())

				if funcCall.STAR() != nil {
					argument = funcCall.STAR().GetText()
					// selectedColumns.Alias = funcName
				} else {
					if funcCall.FunctionArgs() == nil {
						return response, errors.New("function call argument object is nil")
					}
					argument = funcCall.FunctionArgs().GetText()
				}
				selectedColumns.Name = funcName + "_" + argument
				selectedColumns.FuncName = funcName
				selectedColumns.FuncColumnName = argument
				asOperator := val.KwAs()
				if asOperator != nil {
					selectedColumns.IsAs = true
					selectedColumns.Alias = val.AllOBJECT_NAME()[0].GetText()
				}
				response.Column = append(response.Column, selectedColumns)
				continue
			}
			if strings.Contains(selectedColumns.Name, missingUndefined) {
				return response, errors.New("one or more undefined fields for column name")
			}
			allObject := val.AllOBJECT_NAME()
			asOperator := val.KwAs()
			if asOperator != nil && len(allObject) != 2 {
				return response, errors.New("unknown flow with as operator")
			}
			if asOperator != nil {
				selectedColumns.IsAs = true
				selectedColumns.Name = allObject[0].GetText()
				selectedColumns.Alias = allObject[1].GetText()
			} else if len(allObject) == 2 {
				columnName := allObject[0].GetText()
				s := strings.ToLower(selectedColumns.Name)
				if strings.Contains(s, "writetime("+columnName+")") {
					selectedColumns.Name = "writetime(" + columnName + ")"
					selectedColumns.Alias = allObject[1].GetText()
					selectedColumns.IsAs = true
					selectedColumns.FuncColumnName = columnName
					selectedColumns.IsWriteTimeColumn = true
				}
			}
			selectedColumns.Name = strings.ReplaceAll(selectedColumns.Name, literalPlaceholder, "")
			writetime_value, isExist := ExtractWritetimeValue(selectedColumns.Name)
			if isExist {
				selectedColumns.IsWriteTimeColumn = true
				selectedColumns.FuncColumnName = writetime_value
			}
			response.Column = append(response.Column, selectedColumns)
		}
	}
	return response, nil
}

// parseTableFromSelect() parse Table Name from the Select Query
//
// Parameters:
//   - input: The From Spec context from the antlr Parser.
//
// Returns: Table Name and an error if any.
func parseTableFromSelect(input cql.IFromSpecContext) (*TableObj, error) {
	if input == nil {
		return nil, errors.New("no input parameters found for table and keyspace")
	}

	var response TableObj
	fromSpec := input.FromSpecElement()
	if fromSpec == nil {
		return nil, errors.New("error while parsing fromSpec")
	}
	allObj := fromSpec.AllOBJECT_NAME()
	if allObj == nil {
		return nil, errors.New("error while parsing all objects from the fromSpec")
	}

	if len(allObj) == 0 {
		return nil, errors.New("could not find table and keyspace name")
	}

	var tableObj antlr.TerminalNode
	var keyspaceObj antlr.TerminalNode
	if len(allObj) == 2 {
		keyspaceObj = fromSpec.OBJECT_NAME(0)
		tableObj = fromSpec.OBJECT_NAME(1)
		response.TableName = tableObj.GetText()
		response.KeyspaceName = keyspaceObj.GetText()
	} else {
		return nil, errors.New("could not find table or keyspace name or some extra parameter provided")
	}
	return &response, nil
}

// parseOrderByFromSelect() parse Order By from the Select Query
//
// Parameters:
//   - input: The Order Spec context from the antlr Parser.
//
// Returns: OrderBy struct
func parseOrderByFromSelect(input cql.IOrderSpecContext) (OrderBy, error) {
	var response OrderBy

	if input == nil {
		response.IsOrderBy = false
		return response, nil
	}

	orderSpecElement := input.OrderSpecElement()

	if orderSpecElement == nil {
		return response, fmt.Errorf("Order_by section not have proper values")
	}
	object := orderSpecElement.OBJECT_NAME()

	if object == nil {
		return response, fmt.Errorf("Order_by section not have proper values")
	}
	colName := object.GetText()
	if strings.Contains(colName, missingUndefined) {
		return response, fmt.Errorf("In order by, column name not provided correctly")
	}
	response.IsOrderBy = true
	colName = strings.ReplaceAll(colName, literalPlaceholder, "")
	response.Column = colName

	response.Operation = Asc
	if input.OrderSpecElement().KwDesc() != nil {
		response.Operation = Desc
	}

	return response, nil
}

// parseLimitFromSelect() parse Limit from the Select Query
//
// Parameters:
//   - input: The Limit Spec context from the antlr Parser.
//
// Returns: Limit struct
func parseLimitFromSelect(input cql.ILimitSpecContext) (Limit, error) {
	var response Limit

	if input == nil {
		return response, nil
	}

	limitVal := input.DecimalLiteral().GetText()
	if limitVal == "" {
		return response, fmt.Errorf("LIMIT must be strictly positive")
	} else if strings.Contains(limitVal, missingUndefined) {
		return response, nil
	}
	// Check if the limit value is numeric and non-zero.
	if !strings.Contains(limitVal, "?") {
		// Attempt to convert count to an integer.
		numericCount, err := strconv.Atoi(limitVal)

		// Check if there was an error in conversion or if the numeric count is zero or negative.
		if err != nil || numericCount <= 0 {
			return response, fmt.Errorf("no viable alternative at input '%s'", limitVal)
		}
	}

	response.IsLimit = true
	response.Count = limitVal
	return response, nil
}

func parseGroupByColumn(lowerQuery string) []string {
	// TODO: imrove parser to improve this. currently the parser is not able to parse group by clause
	// Find GROUP BY keyword
	groupByIndex := strings.Index(lowerQuery, "group by")
	if groupByIndex == -1 {
		// No GROUP BY clause found
		return nil
	}

	// Get substring after GROUP BY
	remainingQuery := lowerQuery[groupByIndex+8:]

	// Find the end of the GROUP BY clause
	endIndex := -1
	for _, term := range []string{" order by ", " limit ", ";"} {
		idx := strings.Index(remainingQuery, term)
		if idx != -1 && (endIndex == -1 || idx < endIndex) {
			endIndex = idx
		}
	}

	if endIndex != -1 {
		remainingQuery = remainingQuery[:endIndex]
	}

	// Split on commas and clean up each column
	columns := strings.Split(remainingQuery, ",")
	var cleanColumns []string

	for _, col := range columns {
		trimmed := strings.TrimSpace(col)
		if trimmed != "" {
			cleanColumns = append(cleanColumns, trimmed)
		}
	}

	return cleanColumns
}

// processStrings() processes the selected columns, formats them, and returns a map of aliases to metadata and a slice of formatted columns.
// Parameters:
//   - t : Translator instance
//   - selectedColumns: []schemaMapping.SelectedColumns
//   - tableName : table name on which query is being executes
//   - keySpace : keyspace name on which query is being executed
//
// Returns:
//   - []string column containing formatted selected columns for bigtable query
//   - map[string]AsKeywordMeta containing alias to metadata mapping
//   - error if any
func processStrings(t *Translator, selectedColumns []schemaMapping.SelectedColumns, tableName string, keySpace string, isGroupBy bool) (map[string]AsKeywordMeta, []string, error) {
	lenInputString := len(selectedColumns)
	var columns = make([]string, 0)
	structs := make([]AsKeywordMeta, 0, lenInputString)
	columnFamily := t.SchemaMappingConfig.SystemColumnFamily
	var err error
	for _, columnMetadata := range selectedColumns {
		dataType := ""
		isStructUpdated := false
		if columnMetadata.IsFunc {
			//todo: implement genereralized handling of writetime with rest of the aggregate functions
			columns, dataType, err = processFunctionColumn(t, columnMetadata, tableName, keySpace, columns)
			if err != nil {
				return nil, nil, err
			}
			if !isStructUpdated {
				structs = append(structs, AsKeywordMeta{
					IsFunc:   columnMetadata.IsFunc,
					Name:     columnMetadata.FuncColumnName,
					Alias:    columnMetadata.Alias,
					DataType: dataType,
				})
			}
			continue
		} else {
			columns, structs, dataType, isStructUpdated, err = processNonFunctionColumn(t, columnMetadata, tableName, keySpace, columnFamily, columns, structs, isGroupBy)
			if err != nil {
				return nil, nil, err
			}
		}
		if !isStructUpdated {
			structs = append(structs, AsKeywordMeta{
				IsFunc:   columnMetadata.IsFunc,
				Name:     columnMetadata.Name,
				Alias:    columnMetadata.Alias,
				DataType: dataType,
			})
		}
	}
	var aliasMap = make(map[string]AsKeywordMeta)
	for _, meta := range structs {
		if meta.Alias != "" {
			aliasMap[meta.Alias] = meta
		}
	}
	return aliasMap, columns, nil
}

// processFunctionColumn processes columns that have aggregate functions applied to them in a SELECT query.
// It handles special cases like COUNT(*) and validates if the column and function types are allowed in aggregates.
//
// Parameters:
//   - t: Translator instance containing schema mapping configuration
//   - columnMetadata: Contains information about the selected column including function name and aliases
//   - tableName: Name of the table being queried
//   - keySpace: Keyspace name where the table exists
//   - columns: Slice of strings containing the processed column expressions
//
// Returns:
//   - []string: Updated slice of columns with the processed function column
//   - string: The data type of the column after function application
//   - error: Error if column metadata is not found, or if column/function type is not supported for aggregation
//
// The function performs the following operations:
//  1. Handles COUNT(*) as a special case
//  2. Validates column existence in schema mapping
//  3. Checks if column data type is allowed in aggregates
//  4. Validates if the aggregate function is supported
//  5. Applies any necessary type casting (converting cql select columns to respective bigtable columns)
//  6. Formats the column expression with function and alias if specified
func processFunctionColumn(t *Translator, columnMetadata schemaMapping.SelectedColumns, tableName string, keySpace string, columns []string) ([]string, string, error) {

	if columnMetadata.FuncColumnName == STAR && strings.ToLower(columnMetadata.FuncName) == "count" {
		columns = append(columns, "count(*)")
		return columns, "bigint", nil

	}
	colMeta, found := t.SchemaMappingConfig.TablesMetaData[keySpace][tableName][columnMetadata.FuncColumnName]
	if !found {
		// Check if the column is an alias
		if aliasMeta, aliasFound := t.SchemaMappingConfig.TablesMetaData[keySpace][tableName][columnMetadata.Alias]; aliasFound {
			colMeta = aliasMeta
		} else if columnMetadata.FuncName == "count" && columnMetadata.FuncColumnName == STAR {
			// Handle special case for count(*)
			return append(columns, "count(*)"), "bigint", nil
		} else {
			return nil, "", fmt.Errorf("column metadata not found for column '%s' in table '%s' and keyspace '%s'", columnMetadata.FuncColumnName, tableName, keySpace)
		}
	}
	colFamiliy := t.SchemaMappingConfig.SystemColumnFamily
	dataType := colMeta.CQLType
	column := ""

	if !funcAllowedInAggregate(columnMetadata.FuncName) {
		return nil, "", fmt.Errorf("unknown function '%s'", columnMetadata.FuncName)
	}
	if columnMetadata.FuncName != "count" {
		if !dtAllowedInAggregate(colMeta.ColumnType) {
			return nil, "", fmt.Errorf("column not supported for aggregate")
		}
	}
	castValue, castErr := castColumns(colMeta, colFamiliy)
	if castErr != nil {
		return nil, "", castErr
	}
	column = fmt.Sprintf("%s(%s)", columnMetadata.FuncName, castValue)

	if columnMetadata.IsAs {
		column = column + " as " + columnMetadata.Alias
	}
	columns = append(columns, column)
	return columns, dataType, nil
}

// dtAllowedInAggregate checks whether the provided data type is allowed in aggregate functions.
// It returns true if dataType is one of the supported numeric types (i.e., "int", "bigint", "float", or "double"),
// ensuring that only appropriate types are used for aggregate operations.
func dtAllowedInAggregate(dataType string) bool {
	allowedDataTypes := map[string]bool{
		"int":    true,
		"bigint": true,
		"float":  true,
		"double": true,
	}
	return allowedDataTypes[dataType]
}

// funcAllowedInAggregate checks if a given function name is allowed within an aggregate function.
// It converts the input string to lowercase and checks if it exists as a key in the allowedFunctions map.
// The allowed functions are "avg", "sum", "min", "max", and "count".
// It returns true if the function is allowed, and false otherwise.
func funcAllowedInAggregate(s string) bool {
	s = strings.ToLower(s)
	allowedFunctions := map[string]bool{
		"avg":   true,
		"sum":   true,
		"min":   true,
		"max":   true,
		"count": true,
	}

	return allowedFunctions[s]
}

func processNonFunctionColumn(t *Translator, columnMetadata schemaMapping.SelectedColumns, tableName string, keySpace string, columnFamily string, columns []string, structs []AsKeywordMeta, isGroupBy bool) ([]string, []AsKeywordMeta, string, bool, error) {
	dataType := ""
	isStructUpdated := false
	var colMeta *schemaMapping.Column
	var ok bool
	colName := columnMetadata.Name
	if columnMetadata.IsWriteTimeColumn {
		colName = columnMetadata.FuncColumnName
		dataType = "timestamp"
	}
	if columnMetadata.MapKey != "" {
		colName = columnMetadata.MapColumnName
	}
	colMeta, ok = t.SchemaMappingConfig.TablesMetaData[keySpace][tableName][colName]
	if !ok {
		return nil, nil, "", false, fmt.Errorf("column metadata not found for col %s.%s", tableName, colName)
	}
	if !columnMetadata.IsWriteTimeColumn {
		dataType = colMeta.CQLType
	}
	if columnMetadata.IsWriteTimeColumn {
		columns, structs, isStructUpdated = processWriteTimeColumn(t, columnMetadata, tableName, keySpace, columnFamily, columns, structs)
	} else if columnMetadata.IsAs {
		columns = processAsColumn(columnMetadata, tableName, columnFamily, colMeta, columns, isGroupBy)
	} else {
		columns = processRegularColumn(columnMetadata, tableName, columnFamily, colMeta, columns, isGroupBy)
	}
	return columns, structs, dataType, isStructUpdated, nil
}

func processWriteTimeColumn(t *Translator, columnMetadata schemaMapping.SelectedColumns, tableName string, keySpace string, columnFamily string, columns []string, structs []AsKeywordMeta) ([]string, []AsKeywordMeta, bool) {
	colFamily := t.GetColumnFamily(tableName, columnMetadata.FuncColumnName, keySpace)
	aliasColumnName := customWriteTime + columnMetadata.FuncColumnName + ""
	if columnMetadata.Alias == customWriteTime+columnMetadata.FuncColumnName {
		columnMetadata.FormattedColumn = fmt.Sprintf("WRITE_TIMESTAMP(%s, '%s') as %s", colFamily, columnMetadata.FuncColumnName, aliasColumnName)
	} else if columnMetadata.Alias != "" {
		columnMetadata.FormattedColumn = fmt.Sprintf("WRITE_TIMESTAMP(%s, '%s') as %s", columnFamily, columnMetadata.FuncColumnName, columnMetadata.Alias)
		aliasColumnName = columnMetadata.Alias
	} else {
		columnMetadata.FormattedColumn = fmt.Sprintf("WRITE_TIMESTAMP(%s, '%s')", columnFamily, columnMetadata.FuncColumnName)
		aliasColumnName = ""
	}
	structs = append(structs, AsKeywordMeta{
		IsFunc:   columnMetadata.IsFunc,
		Name:     columnMetadata.FuncColumnName,
		Alias:    aliasColumnName,
		DataType: "timestamp",
	})
	columns = append(columns, columnMetadata.FormattedColumn)
	return columns, structs, true
}

func processAsColumn(columnMetadata schemaMapping.SelectedColumns, tableName string, columnFamily string, colMeta *schemaMapping.Column, columns []string, isGroupBy bool) []string {
	var columnSelected string
	if !colMeta.IsCollection {
		if isGroupBy {
			castedCol, _ := castColumns(colMeta, columnFamily)
			columnSelected = castedCol + " as " + columnMetadata.Alias
		} else {
			columnSelected = fmt.Sprintf("%s['%s'] as %s", columnFamily, columnMetadata.Name, columnMetadata.Alias)
		}
	} else {
		columnSelected = fmt.Sprintf("`%s` as %s", columnMetadata.Name, columnMetadata.Alias)
		if columnMetadata.MapKey != "" {
			columnSelected = fmt.Sprintf("%s['%s'] as %s", columnMetadata.MapColumnName, columnMetadata.MapKey, columnMetadata.Alias)
		}
	}
	columns = append(columns, columnSelected)
	return columns
}

/*
processRegularColumn processes the given column based on its metadata and table context.

It formats the column name differently depending on whether the current table name matches the column metadata's name.
If there is a match, the function qualifies the column name using the table name; otherwise, it formats the column name as standalone.
Additionally, if the column is not a collection, it builds a formatted reference that includes accessing the column within the specified column family;
if the column is a collection, it uses the preformatted column name from the metadata.

Parameters:
  - columnMetadata: Specifies metadata for the selected column, including its name and a field for storing the formatted version.
  - tableName: The name of the current table to determine if table-qualified formatting is needed.
  - columnFamily: The column family identifier used when constructing the column reference for non-collection types.
  - colMeta: A pointer to column configuration containing additional details such as whether the column is a collection.
  - columns: A slice of strings that accumulates the formatted column references.

Returns:

	An updated slice of strings with the new formatted column reference appended.
*/
func processRegularColumn(columnMetadata schemaMapping.SelectedColumns, tableName string, columnFamily string, colMeta *schemaMapping.Column, columns []string, isGroupBy bool) []string {
	if !colMeta.IsCollection {
		if isGroupBy {
			castedCol, _ := castColumns(colMeta, columnFamily)
			columns = append(columns, castedCol)
		} else if colMeta.IsPrimaryKey {
			columns = append(columns, columnMetadata.Name)
		} else {
			columns = append(columns, fmt.Sprintf("%s['%s']", columnFamily, columnMetadata.Name))
		}
	} else {
		collectionColumn := "`" + columnMetadata.Name + "`"
		if columnMetadata.MapKey != "" {
			collectionColumn = fmt.Sprintf("%s['%s']", columnMetadata.MapColumnName, columnMetadata.MapKey)
		}
		columns = append(columns, collectionColumn)
	}
	return columns
}

// inferDataType() returns the data type based on the name of a function.
//
// Parameters:
//   - methodName: Name of aggregate function
//
// Returns: Returns datatype of aggregate function.
func inferDataType(methodName string) (string, error) {
	switch methodName {
	case "count":
		return "bigint", nil
	case "round":
		return "float", nil
	default:
		return "", fmt.Errorf("unknown function '%s'", methodName)
	}
}

// getBigtableSelectQuery() Returns Bigtable Select query using Parsed information.
//
// Parameters:
//   - data: SelectQueryMap struct with all select query info from CQL query
func getBigtableSelectQuery(t *Translator, data *SelectQueryMap) (string, error) {
	column := ""
	var columns []string
	var err error
	var aliasMap map[string]AsKeywordMeta

	if data.ColumnMeta.Star {
		column = STAR
	} else {
		isGroupBy := false
		if len(data.GroupByColumns) > 0 {
			isGroupBy = true
		}
		aliasMap, columns, err = processStrings(t, data.ColumnMeta.Column, data.Table, data.Keyspace, isGroupBy)
		if err != nil {
			return "nil", err
		}
		data.AliasMap = aliasMap
		column = strings.Join(columns, ",")
	}

	if column == "" && data.Table == "" {
		return "", errors.New("could not prepare the select query due to incomplete information")
	}
	btQuery := fmt.Sprintf("SELECT %s FROM %s", column, data.Table)
	whereCondition, err := buildWhereClause(data.Clauses, t, data.Table, data.Keyspace)
	if err != nil {
		return "nil", err
	}
	if whereCondition != "" {
		btQuery += whereCondition
	}

	if data.OrderBy.IsOrderBy {
		if colMeta, ok := t.SchemaMappingConfig.TablesMetaData[data.Keyspace][data.Table][data.OrderBy.Column]; ok {
			if colMeta.IsPrimaryKey {
				btQuery = btQuery + " ORDER BY " + data.OrderBy.Column + " " + string(data.OrderBy.Operation)
			} else if !colMeta.IsCollection {
				orderByKey := t.SchemaMappingConfig.SystemColumnFamily + "['" + data.OrderBy.Column + "']"
				btQuery = btQuery + " ORDER BY " + orderByKey + " " + string(data.OrderBy.Operation)
			} else {
				return "", errors.New("order by on collection data type is not supported")
			}
		} else {
			return "", errors.New("Undefined column name " + data.OrderBy.Column + " in table " + data.Keyspace + "." + data.Table)
		}

	}

	if data.Limit.IsLimit {
		val := data.Limit.Count
		if val == questionMark || strings.Contains(val, questionMark) {
			val = "@" + limitPlaceholder
		}
		btQuery = btQuery + " " + "LIMIT" + " " + val
	}
	if len(data.GroupByColumns) > 0 {
		btQuery = btQuery + " GROUP BY "
		groupBykeys := []string{}
		for _, col := range data.GroupByColumns {
			if colMeta, ok := t.SchemaMappingConfig.TablesMetaData[data.Keyspace][data.Table][col]; ok {
				if !colMeta.IsCollection {
					col, err := castColumns(colMeta, t.SchemaMappingConfig.SystemColumnFamily)
					if err != nil {
						return "", err
					}
					groupBykeys = append(groupBykeys, col)
				} else {
					return "", errors.New("group by on collection data type is not supported")
				}
			}
		}
		btQuery = btQuery + strings.Join(groupBykeys, ",")
	}
	btQuery += ";"
	return btQuery, nil

}

// TranslateSelectQuerytoBigtable() Translates Cassandra select statement into a compatible Cloud Bigtable select query.
//
// Parameters:
//   - query: CQL Select statement
//
// Returns: SelectQueryMap struct and error if any
func (t *Translator) TranslateSelectQuerytoBigtable(originalQuery string) (*SelectQueryMap, error) {
	lowerQuery := strings.ToLower(originalQuery)

	//Create copy of cassandra query where literals are substituted with a suffix
	query := renameLiterals(originalQuery)

	p, err := NewCqlParser(query, false)
	if err != nil {
		return nil, err
	}
	selectObj := p.Select_()
	if selectObj == nil {
		// Todo - Code is not reachable
		return nil, errors.New("ToBigtableSelect: Could not parse select object")
	}

	kwSelectObj := selectObj.KwSelect()
	if kwSelectObj == nil {
		// Todo - Code is not reachable
		return nil, errors.New("ToBigtableSelect: Could not parse select object")
	}

	queryType := kwSelectObj.GetText()
	columns, err := parseColumnsFromSelect(selectObj.SelectElements())
	if err != nil {
		return nil, err
	}

	tableSpec, err := parseTableFromSelect(selectObj.FromSpec())
	if err != nil {
		return nil, err
	}
	if !t.SchemaMappingConfig.InstanceExists(tableSpec.KeyspaceName) {
		return nil, fmt.Errorf("keyspace %s does not exist", tableSpec.KeyspaceName)
	}
	if !t.SchemaMappingConfig.TableExist(tableSpec.KeyspaceName, tableSpec.TableName) {
		return nil, fmt.Errorf("table %s does not exist", tableSpec.TableName)
	}
	var QueryClauses QueryClauses

	if hasWhere(lowerQuery) {
		resp, err := parseWhereByClause(selectObj.WhereSpec(), tableSpec.TableName, t.SchemaMappingConfig, tableSpec.KeyspaceName)
		if err != nil {
			return nil, err
		}
		QueryClauses = *resp
	}

	var groupBy []string
	if hasGroupBy(lowerQuery) {
		groupBy = parseGroupByColumn(lowerQuery)
	}
	var orderBy OrderBy
	if hasOrderBy(lowerQuery) {
		orderBy, err = parseOrderByFromSelect(selectObj.OrderSpec())
		if err != nil {
			// pass the original error to provide proper root cause of error.
			return nil, err
		}
	} else {
		orderBy.IsOrderBy = false
	}

	var limit Limit
	if hasLimit(lowerQuery) {
		limit, err = parseLimitFromSelect(selectObj.LimitSpec())
		if err != nil {
			return nil, err
		}
		if limit.Count == questionMark || strings.Contains(limit.Count, questionMark) {
			QueryClauses.ParamKeys = append(QueryClauses.ParamKeys, limitPlaceholder)
		}
	} else {
		limit.IsLimit = false
	}

	if limit.IsLimit && !orderBy.IsOrderBy {
		// Todo:- Add this logic in the parser
		limitPosition := strings.Index(strings.ToUpper(query), "LIMIT")
		orderByPosition := strings.Index(strings.ToUpper(query), "ORDER BY")
		if orderByPosition > limitPosition {
			return nil, errors.New("mismatched input 'Order' expecting EOF (...age = ? LIMIT ? [Order]...)")
		}
	}

	pmks, err := t.SchemaMappingConfig.GetPkByTableName(tableSpec.TableName, tableSpec.KeyspaceName)
	if err != nil {
		return nil, err
	}

	var pmkNames []string
	for _, pmk := range pmks {
		pmkNames = append(pmkNames, pmk.ColumnName)
	}

	selectQueryData := &SelectQueryMap{
		Query:           query,
		TranslatedQuery: "",
		QueryType:       queryType,
		Table:           tableSpec.TableName,
		Keyspace:        tableSpec.KeyspaceName,
		ColumnMeta:      columns,
		Clauses:         QueryClauses.Clauses,
		PrimaryKeys:     pmkNames,
		Limit:           limit,
		OrderBy:         orderBy,
		GroupByColumns:  groupBy,
		Params:          QueryClauses.Params,
		ParamKeys:       QueryClauses.ParamKeys,
	}

	translatedResult, err := getBigtableSelectQuery(t, selectQueryData)
	if err != nil {
		return nil, err
	}

	selectQueryData.TranslatedQuery = translatedResult
	return selectQueryData, nil
}
