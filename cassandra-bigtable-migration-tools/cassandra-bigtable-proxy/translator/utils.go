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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/bigtable"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/collectiondecoder"
	schemaMapping "github.com/ollionorg/cassandra-to-bigtable-proxy/schema-mapping"
	cql "github.com/ollionorg/cassandra-to-bigtable-proxy/third_party/cqlparser"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/utilities"
)

const (
	literalPlaceholder  = "_a1b2c3"
	ttlValuePlaceholder = "ttlValue"
	tsValuePlaceholder  = "tsValue"
	bigtableTSColumn    = "last_commit_ts"
	limitPlaceholder    = "limitValue"
	writetime           = "writetime"
	commitTsFn          = "PENDING_COMMIT_TIMESTAMP()"
	missingUndefined    = "<missing undefined>"
	questionMark        = "?"
	STAR                = "*"
	maxNanos            = int32(9999)
	referenceTime       = int64(1262304000000)
	ifExists            = "ifexists"
	rowKeyJoinString    = "#"
)

var (
	whereRegex          = regexp.MustCompile(`(?i)\bwhere\b`)
	orderByRegex        = regexp.MustCompile(`(?i)\border\s+by\b`)
	groupByRegex        = regexp.MustCompile(`(?i)\bgroup\s+by\b`)
	limitRegex          = regexp.MustCompile(`(?i)\blimit\b`)
	usingTimestampRegex = regexp.MustCompile(`using.*timestamp`)
	combinedRegex       = createCombinedRegex()
)

var (
	ErrEmptyTable           = errors.New("could not find table name to create spanner query")
	ErrParsingDelObj        = errors.New("error while parsing delete object")
	ErrParsingTs            = errors.New("timestamp could not be parsed")
	ErrTsNoValue            = errors.New("no value found for Timestamp")
	ErrEmptyTableOrKeyspace = errors.New("ToSpannerDelete: No table or keyspace name found in the query")
)

// convertMapString(): Function to convert map data into String
func convertMapString(dataStr string) (map[string]interface{}, error) {
	// Split the string into key-value pairs
	pairs := strings.Split(strings.Trim(dataStr, "{}"), ", ")

	// Initialize an empty map for storing data
	data := map[string]interface{}{}

	// // Extract key-value pairs and add them to the map
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			parts = strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				// Handle the case where the split doesn't result in 2 parts
				return nil, fmt.Errorf("error while formatting %v", parts)
			}
		}

		v := strings.TrimSpace(strings.ToLower(parts[1]))
		k := strings.ReplaceAll(parts[0], `"`, ``)
		if v == "true" {
			data[k] = true
		} else {
			data[k] = false
		}

	}

	return data, nil
}

// convertToMap(): function to convert json tring to cassandra map type
func convertToMap(dataStr string, cqlType string) (map[string]interface{}, error) {
	// Split the string into key-value pairs
	dataStr = strings.ReplaceAll(dataStr, ": \"", ":\"")
	dataStr = strings.ReplaceAll(dataStr, "\"", "")
	pairs := strings.Split(strings.Trim(dataStr, "{}"), ", ")
	// Initialize an empty map for storing data
	switch cqlType {
	case "map<varchar, timestamp>":
		return mapOfStringToTimestamp(pairs)
	case "map<varchar, varchar>":
		return mapOfStringToString(pairs)
	default:
		return nil, fmt.Errorf("unhandled map type")
	}
}

// mapOfStringToString(): Converts a slice of key-value pairs into a map with string keys and string values.
func mapOfStringToString(pairs []string) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			parts = strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				// Handle the case where the split doesn't result in 2 parts
				return nil, fmt.Errorf("error while formatting %v", parts)
			}

		}
		data[parts[0]] = parts[1]
	}
	return data, nil
}

// mapOfStringToTimestamp(): Converts a slice of key-value pairs into a map with string keys and timestamp values.
func mapOfStringToTimestamp(pairs []string) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			parts = strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				// Handle the case where the split doesn't result in 2 parts
				return nil, fmt.Errorf("error while formatting %v", parts)
			}
		}
		timestamp, err := parseTimestamp(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("error while typecasting to timestamp %v, -> %s", parts[1], err)
		}
		data[parts[0]] = timestamp
	}
	return data, nil
}

// hasUsingTimestamp(): Function to check if using timestamp exist in the query.
func hasUsingTimestamp(query string) bool {
	return usingTimestampRegex.MatchString(query)
}

// parseTimestamp(): Parse a timestamp string in various formats.
// Supported formats
// "2024-02-05T14:00:00Z",
// "2024-02-05 14:00:00",
// "2024/02/05 14:00:00",
// "1672522562",             // Unix timestamp (seconds)
// "1672522562000",          // Unix timestamp (milliseconds)
// "1672522562000000",       // Unix timestamp (microseconds)
func parseTimestamp(timestampStr string) (time.Time, error) {
	// Define multiple layouts to try
	layouts := []string{
		time.RFC3339,          // ISO 8601 format
		"2006-01-02 15:04:05", // Common date-time format
		"2006/01/02 15:04:05", // Common date-time format with slashes
	}

	var parsedTime time.Time
	var err error

	// Try to parse the timestamp using each layout
	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, timestampStr)
		if err == nil {
			return parsedTime, nil
		}
	}

	// Try to parse as Unix timestamp (in seconds, milliseconds, or microseconds)
	if unixTime, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
		switch len(timestampStr) {
		case 10: // Seconds
			return time.Unix(unixTime, 0).UTC(), nil
		case 13: // Milliseconds
			return time.Unix(0, unixTime*int64(time.Millisecond)).UTC(), nil
		case 16: // Microseconds
			return time.Unix(0, unixTime*int64(time.Microsecond)).UTC(), nil
		}
	}

	// If all formats fail, return the last error
	return time.Time{}, err
}

// treetoString(): Function to parse string from ANTLR tree
func treetoString(tree antlr.Tree) string {
	if (reflect.TypeOf(tree) == reflect.TypeOf(&antlr.TerminalNodeImpl{})) {
		token := tree.(antlr.TerminalNode).GetSymbol()
		return token.GetText()

	}
	return ""
}

// hasWhere(): Function to check if where clause exist in the query.
func hasWhere(query string) bool {
	return whereRegex.MatchString(query)
}

// hasOrderBy(): Function to check if order by exist in the query.
func hasOrderBy(query string) bool {
	return orderByRegex.MatchString(query)
}

// hasGroupBy(): Function to check if group by exist in the query.
func hasGroupBy(query string) bool {
	return groupByRegex.MatchString(query)
}

// hasLimit(): Function to check if limit exist in the query.
func hasLimit(query string) bool {
	return limitRegex.MatchString(query)
}

// formatValues(): Function to encode the cassandra values into bigEndian octal format.
func formatValues(value string, cqlType string, protocolV primitive.ProtocolVersion) ([]byte, error) {
	var iv interface{}
	var dt datatype.DataType

	switch cqlType {
	case "int":
		return EncodeBigInt(value, protocolV)
	case "bigint":
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting string to int64: %w", err)
		}
		iv = val
		dt = datatype.Bigint

	case "float":
		val, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf("error converting string to float32: %w", err)
		}
		iv = float32(val)
		dt = datatype.Float
	case "double":
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting string to float64: %w", err)
		}
		iv = val
		dt = datatype.Double

	case "boolean":
		return EncodeBool(value, protocolV)
	case "timestamp":
		val, err := parseTimestamp(value)
		if err != nil {
			return nil, fmt.Errorf("error converting string to timestamp: %w", err)
		}
		iv = val
		dt = datatype.Timestamp
	case "blob":
		iv = value
		dt = datatype.Blob
	case "text", "varchar":
		iv = value
		dt = datatype.Varchar

	default:
		return nil, fmt.Errorf("unsupported CQL type: %s", cqlType)
	}

	bd, err := proxycore.EncodeType(dt, protocolV, iv)
	if err != nil {
		return nil, fmt.Errorf("error encoding value: %w", err)
	}

	return bd, nil

}

// primitivesToString(): Function used to  Convert primitive values into string
func primitivesToString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int32:
		return strconv.Itoa(int(v)), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return "", fmt.Errorf("unsupported type: %T", v)
	}
}

// stringToPrimitives(): Converts a string value to its corresponding primitive type based on the CQL type.
func stringToPrimitives(value string, cqlType string) (interface{}, error) {
	var iv interface{}

	switch cqlType {
	case "int":
		val, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("error converting string to int32: %w", err)
		}
		iv = int32(val)

	case "bigint":
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting string to int64: %w", err)
		}
		iv = val

	case "float":
		val, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf("error converting string to float32: %w", err)
		}
		iv = float32(val)

	case "double":
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting string to float64: %w", err)
		}
		iv = val
	case "boolean":
		val, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("error converting string to bool: %w", err)
		}
		iv = val

	case "timestamp":
		val, err := parseTimestamp(value)

		if err != nil {
			return nil, fmt.Errorf("error converting string to timestamp: %w", err)
		}
		iv = val
	case "blob":
		iv = value
	case "text", "varchar":
		iv = value

	default:
		return nil, fmt.Errorf("unsupported CQL type: %s", cqlType)

	}
	return iv, nil
}

// createCombinedRegex(): constructs a regular expression that matches any of the specified keywords as whole words.
// The keywords are "time", "key", "type", and "json".
// The resulting regex will match these keywords only if they appear as complete words in the input string.
//
// Returns:
//
//	*regexp.Regexp: A compiled regular expression object.
func createCombinedRegex() *regexp.Regexp {
	keywords := []string{"time", "key", "type", "json"}
	combinedPattern := fmt.Sprintf("\\b(%s)\\b", regexp.QuoteMeta(keywords[0]))

	for _, keyword := range keywords[1:] {
		combinedPattern += fmt.Sprintf("|\\b%s\\b", regexp.QuoteMeta(keyword))
	}
	return regexp.MustCompile(combinedPattern)
}

// renameLiterals(): Function to find and replace the cql literals and add a temporary placeholder.
func renameLiterals(query string) string {
	return combinedRegex.ReplaceAllStringFunc(query, func(match string) string {
		return match + literalPlaceholder
	})
}

// getPrimaryKeys(): Function to get primary keys from schema mapping.
func getPrimaryKeys(schemaMapping *schemaMapping.SchemaMappingConfig, tableName string, keySpace string) ([]string, error) {
	var primaryKeys []string
	pks, err := schemaMapping.GetPkByTableName(tableName, keySpace)
	if err != nil {
		return nil, err
	}

	for _, pk := range pks {
		primaryKeys = append(primaryKeys, pk.ColumnName)
	}

	return primaryKeys, nil
}

// preprocessMapString(): Preprocess the input string to convert it to valid JSON
func preprocessMapString(input string) string {
	input = strings.TrimPrefix(input, "{")
	input = strings.TrimSuffix(input, "}")
	pairs := strings.Split(input, ",")
	for i, pair := range pairs {
		kv := strings.Split(pair, ":")
		if len(kv) != 2 {
			kv = strings.Split(pair, "=")
		}
		if len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			pairs[i] = fmt.Sprintf("\"%s\":\"%s\"", key, value)
		}
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

// preprocessSetString(): Preprocess the set string to convert it to valid JSON
func preprocessSetString(input string) string {
	if (strings.HasPrefix(input, "{") && strings.HasSuffix(input, "}")) || (strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]")) {
		input = input[1 : len(input)-1]
	}
	values := strings.Split(input, ",")
	for i, value := range values {
		value = strings.TrimSpace(value)
		if !strings.HasPrefix(value, "\"") && !strings.HasSuffix(value, "\"") && !strings.HasPrefix(value, "'") && !strings.HasSuffix(value, "'") {
			values[i] = fmt.Sprintf("\"%s\"", value)
		} else {
			values[i] = strings.Trim(value, "'")
			values[i] = fmt.Sprintf("\"%s\"", values[i])
		}
	}
	return "[" + strings.Join(values, ",") + "]"
}

// processCollectionColumnsForRawQueries(): processes columns that involve collection types (e.g., map, set)
// This function operates by iterating over each column provided. If the column represents a collection (e.g., map or set),
// the function decodes the corresponding value (expected to be a JSON string), converts it into the appropriate Go type
// (e.g., map[string]string or []string), and then creates new column-value pairs for each element in the collection.
// Non-collection columns are added directly to the result slices without modification.
//
// Parameters:
// - columns: A slice of Column structures, each representing a column in the database.
// - values: A slice of interface{} values corresponding to each column, containing the raw data to be processed.
// - tableName: The name of the table being processed.
// - t: A pointer to a Translator, which provides utility methods for working with table schemas and collections.
// - keySpace: The name of the keyspace containing the table.
// - prependColumns: A slice of strings representing columns in list which has a prepend operation.
// Returns:
// - A slice of Columns with processed data, reflecting the transformed collections.
// - A slice of interface{} values representing the processed data for each column.
// - A slice of strings representing the column families that have been marked for deletion.
// - A slice of Columns representing the columns that have been marked for deletion.
// - A map of string to ComplexOperation representing the complex update metadata for each column.
// - An error if any step of the processing fails.
func processCollectionColumnsForRawQueries(input ProcessRawCollectionsInput) (*ProcessRawCollectionsOutput, error) {
	output := &ProcessRawCollectionsOutput{
		ComplexMeta: make(map[string]*ComplexOperation),
	}

	for i, column := range input.Columns {
		if column.IsPrimaryKey {
			continue
		}
		if input.Translator.IsCollection(input.KeySpace, input.TableName, column.Name) {
			val := input.Values[i].(string)
			if strings.Contains(val, column.Name) && (strings.Contains(val, "+") || strings.Contains(val, "-")) {
				// in (+) cases we are extracting the value from the collection type from the query. for example map=map+{key:val}
				//  so it extracts {key:val} || {key1,key2} in case of set and list
				if strings.Contains(val, "+") {
					trimmedStr := strings.TrimPrefix(val, column.Name)
					trimmedStr = strings.TrimPrefix(trimmedStr, "+")
					val = strings.TrimSpace(trimmedStr)
				} else if strings.Contains(val, "-") {
					// in (-) cases we are extracting the value from the collection type from the query. for example map=map-{key1,key2}
					// extracts the column family and the element keys for deletion of collection elements. For collection types (e.g., map=map-{key1,key2}),
					// extract both the column family (e.g., 'map') and the element keys (e.g., 'key1', 'key2')
					// to be used for deleting the corresponding columns
					cf, qualifiers, err := ExtractCFAndQualifiersforMap(val)
					if err != nil {
						return nil, fmt.Errorf("error reading map set value: %w", err)
					}
					for _, key := range qualifiers {
						output.DelColumns = append(output.DelColumns, Column{Name: key, ColumnFamily: cf})
					}
					continue
					//once delete keys and cf are captured from the set Item continuing to next one.
				}
			} else {
				// its a deafult block when collection=newcolletion is bheing set. then we are deleting the whole column family and inserting the value
				output.DelColumnFamily = append(output.DelColumnFamily, column.Name)
			}

			colFamily := input.Translator.GetColumnFamily(input.KeySpace, input.TableName, column.Name)
			cols, vals, err := processCollectionByType(val, colFamily, column.CQLType, input.PrependColumns, output.ComplexMeta)
			if err != nil {
				return nil, err
			}
			output.NewColumns = append(output.NewColumns, cols...)
			output.NewValues = append(output.NewValues, vals...)
		} else {
			output.NewColumns = append(output.NewColumns, column)
			output.NewValues = append(output.NewValues, input.Values[i])
		}
	}
	return output, nil
}

// processCollectionByType processes collection-type CQL values and converts them into columns and their corresponding values.
// It handles various CQL collection types including maps, sets, and lists with different value types.
//
// Parameters:
//   - val: The string representation of the collection value to be processed
//   - colFamily: The column family name to be used in the resulting columns
//   - cqlType: The CQL type of the collection (e.g., "map<varchar,varchar>", "set<int>", "list<timestamp>")
//   - prependColumns: Array of column names to be prepended to the resulting columns (used for list processing)
//   - complexMeta: Metadata for complex updates handling
//
// Returns:
//   - []Column: Array of processed columns
//   - []interface{}: Array of corresponding values for the processed columns
//   - error: Error if the CQL type is unsupported or processing fails
//
// Supported CQL types:
//   - Maps: varchar/timestamp keys with varchar, int, bigint, timestamp, float, double, or boolean values
//   - Sets: varchar, boolean, int, bigint, timestamp, float, or double elements
//   - Lists: varchar, int, bigint, boolean, float, double, or timestamp elements
func processCollectionByType(val, colFamily, cqlType string, prependColumns []string, complexMeta map[string]*ComplexOperation) ([]Column, []interface{}, error) {
	switch cqlType {
	case "map<varchar,varchar>", "map<timestamp,varchar>":
		return processMapCollectionRaw(val, colFamily, "varchar")
	case "map<varchar,int>", "map<timestamp,int>":
		return processMapCollectionRaw(val, colFamily, "int")
	case "map<varchar,bigint>", "map<timestamp,bigint>":
		return processMapCollectionRaw(val, colFamily, "bigint")
	case "map<varchar,timestamp>", "map<timestamp,timestamp>":
		return processMapCollectionRaw(val, colFamily, "timestamp")
	case "map<varchar,float>", "map<timestamp,float>":
		return processMapCollectionRaw(val, colFamily, "float")
	case "map<varchar,double>", "map<timestamp,double>":
		return processMapCollectionRaw(val, colFamily, "double")
	case "map<varchar,boolean>", "map<timestamp,boolean>":
		return processMapCollectionRaw(val, colFamily, "boolean")
	case "set<varchar>":
		return processSetCollectionRaw(val, colFamily, "varchar")
	case "set<boolean>":
		return processSetCollectionRaw(val, colFamily, "boolean")
	case "set<int>":
		return processSetCollectionRaw(val, colFamily, "int")
	case "set<bigint>":
		return processSetCollectionRaw(val, colFamily, "bigint")
	case "set<timestamp>":
		return processSetCollectionRaw(val, colFamily, "timestamp")
	case "set<float>":
		return processSetCollectionRaw(val, colFamily, "float")
	case "set<double>":
		return processSetCollectionRaw(val, colFamily, "double")
	case "list<varchar>":
		return processListCollectionRaw(val, colFamily, "varchar", prependColumns, complexMeta)
	case "list<int>":
		return processListCollectionRaw(val, colFamily, "int", prependColumns, complexMeta)
	case "list<bigint>":
		return processListCollectionRaw(val, colFamily, "bigint", prependColumns, complexMeta)
	case "list<boolean>":
		return processListCollectionRaw(val, colFamily, "boolean", prependColumns, complexMeta)
	case "list<float>":
		return processListCollectionRaw(val, colFamily, "float", prependColumns, complexMeta)
	case "list<double>":
		return processListCollectionRaw(val, colFamily, "double", prependColumns, complexMeta)
	case "list<timestamp>":
		return processListCollectionRaw(val, colFamily, "timestamp", prependColumns, complexMeta)
	default:
		return nil, nil, fmt.Errorf("unsupported CQL type: %s", cqlType)
	}
}

// processMapCollectionRaw processes a raw map collection string and converts it into columns and values.
// It takes three parameters:
//   - val: The raw map string value to be processed
//   - colFamily: The column family name
//   - valueType: The type of values in the map (e.g., "boolean", "int", "varchar")
//
// It returns three values:
//   - []Column: Slice of Column structs representing the map entries
//   - []interface{}: Slice of formatted values corresponding to the columns
//   - error: Error if any occurs during processing
//
// The function unmarshals the JSON string into a map, creates Column structs for each key-value pair,
// and formats the values according to the specified valueType. For boolean and int types,
// the CQL type is set to "bigint".
func processMapCollectionRaw(val, colFamily, valueType string) ([]Column, []interface{}, error) {
	var mapValue map[string]string
	formattedJsonStr := preprocessMapString(val)
	if err := json.Unmarshal([]byte(formattedJsonStr), &mapValue); err != nil {
		return nil, nil, fmt.Errorf("error converting string to map<varchar,%s>: %w", valueType, err)
	}
	var cols []Column
	var vals []interface{}
	cqlType := valueType
	if valueType == "boolean" || valueType == "int" {
		cqlType = "bigint"
	}
	for k, v := range mapValue {
		col := Column{
			Name:         k,
			ColumnFamily: colFamily,
			CQLType:      cqlType,
		}
		valueFormatted, err := formatValues(v, valueType, primitive.ProtocolVersion4)
		if err != nil {
			return nil, nil, fmt.Errorf("error converting string to map<varchar,%s> value: %w", valueType, err)
		}
		cols = append(cols, col)
		vals = append(vals, valueFormatted)
	}
	return cols, vals, nil
}

// processSetCollectionRaw processes a set collection value from Cassandra and converts it into Columns and values
// for Bigtable. It handles sets of various value types and performs necessary data conversions.
//
// Parameters:
//   - val: The raw set value from Cassandra as a string
//   - colFamily: The column family name
//   - valueType: The type of values in the set (e.g., "boolean", "varchar")
//
// Returns:
//   - []Column: Slice of Column structs representing the set elements
//   - []interface{}: Slice of corresponding formatted values
//   - error: Error if any occurs during processing or conversion
//
// The function first unmarshals the set string into a slice of strings, then processes each value
// according to its type. For boolean sets, additional type conversion is performed. Each set element
// becomes a separate column in the resulting structure.
func processSetCollectionRaw(val, colFamily, valueType string) ([]Column, []interface{}, error) {
	var setValues []string
	formattedStr := preprocessSetString(val)
	if err := json.Unmarshal([]byte(formattedStr), &setValues); err != nil {
		return nil, nil, fmt.Errorf("error converting string to set<%s>: %w", valueType, err)
	}
	var cols []Column
	var vals []interface{}
	for _, v := range setValues {
		if valueType == "boolean" {
			valInInterface, err := DataConversionInInsertionIfRequired(v, primitive.ProtocolVersion4, "boolean", "string")
			if err != nil {
				return nil, nil, fmt.Errorf("error converting string to set<boolean> value: %w", err)
			}
			v = valInInterface.(string)
		}
		col := Column{
			Name:         v,
			ColumnFamily: colFamily,
			CQLType:      valueType,
		}
		// value parameter is intentionally empty because column identifier holds the value
		valueFormatted, err := formatValues("", "varchar", primitive.ProtocolVersion4)
		if err != nil {
			return nil, nil, fmt.Errorf("error converting string to set<%s> value: %w", valueType, err)
		}
		cols = append(cols, col)
		vals = append(vals, valueFormatted)
	}
	return cols, vals, nil
}

// processListCollectionRaw(): Handles the processing of list-type columns in a CQL query and prepares them for conversion.
func processListCollectionRaw(val, colFamily, cqlType string, prependCol []string, complexMeta map[string]*ComplexOperation) ([]Column, []interface{}, error) {
	var newColumns []Column
	var newValues []interface{}
	var listDelete [][]byte

	formattedStr := preprocessSetString(val)
	var listValues []string
	if err := json.Unmarshal([]byte(formattedStr), &listValues); err != nil {
		return nil, nil, fmt.Errorf("error converting string to list<%s>: %w", cqlType, err)
	}
	prepend := false
	if valExistInArr(prependCol, colFamily) {
		prepend = true
	}
	for i, v := range listValues {
		// Handle list index operation (e.g. "index:value") we are converting index op as this when reading updateElements
		if strings.Contains(v, ":") {
			splitV := strings.SplitN(v, ":", 2)
			if len(splitV) != 2 {
				return nil, nil, fmt.Errorf("invalid list index operation format in value: %s", v)
			}
			index := splitV[0]
			v = splitV[1]
			// Format the value according to the CQL type
			formattedVal, err := formatValues(v, cqlType, primitive.ProtocolVersion4)
			if err != nil {
				return nil, nil, fmt.Errorf("error converting string to list<%s> value: %w", cqlType, err)
			}
			newComplexMeta := ComplexOperation{
				UpdateListIndex: index,
				Value:           formattedVal,
			}
			complexMeta[colFamily] = &newComplexMeta
			// Skip adding this value to newValues/newColumns as it's used for update metadata
			continue
		}

		// Check if this value is marked for deletion in complex update for the list
		if meta, ok := complexMeta[colFamily]; ok && meta.ListDelete {
			formattedVal, err := formatValues(v, cqlType, primitive.ProtocolVersion4)
			if err != nil {
				return nil, nil, fmt.Errorf("error converting string to list<%s> value: %w", cqlType, err)
			}
			listDelete = append(listDelete, formattedVal)
			continue
		}

		// Calculate encoded timestamp for the list element
		encTime := getEncodedTimestamp(i, len(listValues), prepend)

		// Create a new column identifier with the encoded timestamp as the name
		c := Column{
			Name:         string(encTime),
			ColumnFamily: colFamily,
			CQLType:      cqlType,
		}
		// Format the value
		formattedVal, err := formatValues(v, cqlType, primitive.ProtocolVersion4)
		if err != nil {
			return nil, nil, fmt.Errorf("error converting string to list<%s> value: %w", cqlType, err)
		}
		newValues = append(newValues, formattedVal)
		newColumns = append(newColumns, c)
	}

	if len(listDelete) > 0 {
		if meta, ok := complexMeta[colFamily]; ok {
			meta.ListDeleteValues = listDelete
		} else {
			complexMeta[colFamily] = &ComplexOperation{ListDeleteValues: listDelete}
		}
	}

	return newColumns, newValues, nil
}

// processCollectionColumnsForPrepareQueries processes a set of columns and their associated values
// in preparation for generating queries. It handles various CQL collection types such as
// maps and sets, decoding them into Go-native types. This function is particularly useful
// when preparing data for operations like inserts or updates in a Cassandra-like database.
//
// Parameters:
// - columnsResponse: A slice of Column structures, each representing a column in the database.
// - values: A slice of pointers to primitive.Value, representing the values corresponding to each column.
// - tableName: The name of the table being processed.
// - protocolV: The protocol version used for decoding the values.
// - primaryKeys: A slice of strings representing the primary keys for the table.
// - t: A pointer to a Translator, which provides utility methods for working with table schemas and collections.
//
// Returns:
// - A slice of Columns with processed data.
// - A slice of interface{} values representing the processed data for each column.
// - A map[string]interface{} containing the unencrypted values of primary keys.
// - An integer indexEnd representing the last index processed in the columnsResponse.
// - An error if any step of the processing fails.
func processCollectionColumnsForPrepareQueries(input ProcessPrepareCollectionsInput) (*ProcessPrepareCollectionsOutput, error) {
	output := &ProcessPrepareCollectionsOutput{
		Unencrypted: make(map[string]interface{}),
	}
	var compleUpdateMeta *ComplexOperation

	for i, column := range input.ColumnsResponse {
		// todo validate the column exists again, in case the column was dropped after the query was prepared.
		if input.Translator.IsCollection(input.KeySpace, input.TableName, column.Name) {
			colFamily := input.Translator.GetColumnFamily(input.KeySpace, input.TableName, column.Name)
			dt, err := utilities.GetCassandraColumnType(column.CQLType)
			if err != nil {
				return nil, err
			}

			if meta, ok := input.ComplexMeta[column.Name]; ok {
				collectionType := strings.Split(column.CQLType, "<")[0]
				switch collectionType {
				case "list":
					compleUpdateMeta = meta
					if compleUpdateMeta.UpdateListIndex != "" {
						// list index operation e.g. list[index]=val
						// list update for specific index
						listType := ExtractListType(column.CQLType)
						valInInterface, err := DataConversionInInsertionIfRequired(input.Values[i].Contents, input.ProtocolV, listType, "byte")
						if err != nil {
							return nil, fmt.Errorf("error while encoding list<%s> value: %w", listType, err)
						}
						compleUpdateMeta.Value = valInInterface.([]byte)
						continue
					}
					// other cases like prepend, append for list are handled in processListColumnGeneric()
					// append is handled the same as normal list insert but in that case we are not deleting the column family
				case "set": //handling Delete for set e.g set=set-['key1','key2']
					// append case is same as normal set insert just no deletion of column family
					if meta.Delete {
						err := processDeleteOperationForMapAndSet(column, meta, input, i, output)
						if err != nil {
							return nil, err
						}
						continue
					}
				case "map":
					if meta.Append && meta.mapKey != "" { //handling update for specific key e.g map[key]=val
						mapType := dt.(datatype.MapType)
						err := processMapKeyAppend(column, meta, input, i, output, mapType.GetValueType().String())
						if err != nil {
							return nil, err
						}
						continue
					}
					if meta.Delete { //handling Delete for specific key e.g map=map-['key1','key2']
						err := processDeleteOperationForMapAndSet(column, meta, input, i, output)
						if err != nil {
							return nil, err
						}
						continue
					}
				}
			} else {
				// case of when new value is being set for collection type {e.g collection=newCollection}
				output.DelColumnFamily = append(output.DelColumnFamily, column.Name)
			}
			if err := handleCollectionColumn(column, colFamily, input.Values[i], input.ProtocolV, &output.NewColumns, &output.NewValues, compleUpdateMeta); err != nil {
				return nil, err
			}
		} else {
			valInInterface, err := DataConversionInInsertionIfRequired(input.Values[i].Contents, input.ProtocolV, column.CQLType, "byte")
			if err != nil {
				return nil, fmt.Errorf("error converting primitives: %w", err)
			}
			input.Values[i].Contents = valInInterface.([]byte)
			output.NewColumns = append(output.NewColumns, column)
			output.NewValues = append(output.NewValues, input.Values[i].Contents)
			if slices.Contains(input.PrimaryKeys, column.Name) {
				ColPrimitiveType, err := utilities.GetCassandraColumnType(column.CQLType)
				if err != nil {
					return nil, fmt.Errorf("invalid CQL Type %s err: %w", column.CQLType, err)
				}

				val, _ := utilities.DecodeBytesToCassandraColumnType(input.Values[i].Contents, ColPrimitiveType, input.ProtocolV)
				output.Unencrypted[column.Name] = val
			}
		}
		output.IndexEnd = i
	}
	return output, nil
}

// handleCollectionColumn processes a Cassandra collection column (map, set, or list) and converts it into
// appropriate format for storage. It handles various collection types with different value types.
//
// Parameters:
//   - column: Column struct containing information about the collection column
//   - colFamily: String representing the column family name
//   - value: Pointer to primitive.Value containing the collection data
//   - protocolV: Protocol version used for data encoding/decoding
//   - newColumns: Pointer to slice of Column where new column definitions will be added
//   - newValues: Pointer to slice of interface{} where processed values will be stored
//   - complexMeta: Pointer to ComplexOperation containing metadata for complex updates
//
// Returns:
//   - error: Returns an error if processing fails, nil otherwise
//
// The function supports the following collection types:
//   - Maps: text->text, text->(numeric types), text->boolean, timestamp->(all types)
//   - Sets: text, boolean, numeric types, timestamp
//   - Lists: text, boolean, numeric types, timestamp
//
// For unsupported types, it returns an error with the unsupported CQL type.
func handleCollectionColumn(column Column, colFamily string, value *primitive.Value, protocolV primitive.ProtocolVersion, newColumns *[]Column, newValues *[]interface{}, complexMeta *ComplexOperation) error {
	var dataType datatype.DataType
	switch column.CQLType {
	case "map<varchar,timestamp>":
		dataType = utilities.MapOfStrToBigInt
	case "map<timestamp,timestamp>":
		dataType = utilities.MapOfTimeToBigInt
	case "set<timestamp>":
		dataType = utilities.SetOfBigInt
	case "list<timestamp>":
		dataType = utilities.ListOfBigInt
	default:
		dataType, _ = utilities.GetCassandraColumnType(column.CQLType)
	}
	decodedValue, err := decodeCollectionValue(column.CQLType, dataType, value, protocolV)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType, err)
	}

	switch column.CQLType {
	case "map<varchar,varchar>":
		return processTextMapColumn[string](decodedValue, protocolV, colFamily, "varchar", newValues, newColumns)
	case "map<varchar,int>":
		return processTextMapColumn[int32](decodedValue, protocolV, colFamily, "int", newValues, newColumns)
	case "map<varchar,bigint>":
		return processTextMapColumn[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns)
	case "map<varchar,timestamp>":
		return processTextMapColumn[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns)
	case "map<varchar,float>":
		return processTextMapColumn[float32](decodedValue, protocolV, colFamily, "float", newValues, newColumns)
	case "map<varchar,double>":
		return processTextMapColumn[float64](decodedValue, protocolV, colFamily, "double", newValues, newColumns)
	case "map<varchar,boolean>":
		return processTextMapColumn[bool](decodedValue, protocolV, colFamily, "boolean", newValues, newColumns)
	case "map<timestamp,varchar>":
		return processTimestampMapColumn[string](decodedValue, protocolV, colFamily, "varchar", newValues, newColumns)
	case "map<timestamp,int>":
		return processTimestampMapColumn[int32](decodedValue, protocolV, colFamily, "int", newValues, newColumns)
	case "map<timestamp,bigint>":
		return processTimestampMapColumn[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns)
	case "map<timestamp,timestamp>":
		return processTimestampMapColumn[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns)
	case "map<timestamp,float>":
		return processTimestampMapColumn[float32](decodedValue, protocolV, colFamily, "float", newValues, newColumns)
	case "map<timestamp,double>":
		return processTimestampMapColumn[float64](decodedValue, protocolV, colFamily, "double", newValues, newColumns)
	case "map<timestamp,boolean>":
		return processTimestampMapColumn[bool](decodedValue, protocolV, colFamily, "boolean", newValues, newColumns)
	case "set<varchar>":
		return processSetColumnGeneric[string](decodedValue, protocolV, colFamily, "varchar", newValues, newColumns)
	case "set<boolean>":
		return processSetColumnGeneric[bool](decodedValue, protocolV, colFamily, "boolean", newValues, newColumns)
	case "set<int>":
		return processSetColumnGeneric[int32](decodedValue, protocolV, colFamily, "int", newValues, newColumns)
	case "set<bigint>":
		return processSetColumnGeneric[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns)
	case "set<float>":
		return processSetColumnGeneric[float32](decodedValue, protocolV, colFamily, "float", newValues, newColumns)
	case "set<double>":
		return processSetColumnGeneric[float64](decodedValue, protocolV, colFamily, "double", newValues, newColumns)
	case "set<timestamp>":
		return processSetColumnGeneric[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns)
	case "list<varchar>":
		return processListColumnGeneric[string](decodedValue, protocolV, colFamily, "varchar", newValues, newColumns, complexMeta)
	case "list<int>":
		return processListColumnGeneric[int32](decodedValue, protocolV, colFamily, "int", newValues, newColumns, complexMeta)
	case "list<bigint>":
		return processListColumnGeneric[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns, complexMeta)
	case "list<float>":
		return processListColumnGeneric[float32](decodedValue, protocolV, colFamily, "float", newValues, newColumns, complexMeta)
	case "list<double>":
		return processListColumnGeneric[float64](decodedValue, protocolV, colFamily, "double", newValues, newColumns, complexMeta)
	case "list<boolean>":
		return processListColumnGeneric[bool](decodedValue, protocolV, colFamily, "boolean", newValues, newColumns, complexMeta)
	case "list<timestamp>":
		return processListColumnGeneric[int64](decodedValue, protocolV, colFamily, "bigint", newValues, newColumns, complexMeta)
	default:
		return fmt.Errorf("unsupported CQL type: %s", column.CQLType)
	}
}

/*
decodeCollectionValue decodes a collection value based on the CQL type, data type, value, and protocol version.
It handles different types of collections such as maps, sets, and lists, and uses appropriate decoding methods based on the CQL type.
*/
func decodeCollectionValue(cqlType string, dataType datatype.DataType, value *primitive.Value, protocolV primitive.ProtocolVersion) (interface{}, error) {
	switch cqlType {
	case "map<varchar,varchar>", "map<varchar,int>", "map<varchar,bigint>", "map<varchar,float>", "map<varchar,double>", "map<varchar,boolean>", "map<varchar,timestamp>":
		return proxycore.DecodeType(dataType, protocolV, value.Contents)
	case "map<timestamp,timestamp>", "set<timestamp>", "list<timestamp>":
		return collectiondecoder.DecodeCollection(dataType, protocolV, value.Contents)
	default:
		return collectiondecoder.DecodeCollection(dataType, protocolV, value.Contents)
	}
}

// NewCqlParser(): Funtion to create lexer and parser object for the cql query
func NewCqlParser(cqlQuery string, isDebug bool) (*cql.CqlParser, error) {
	if cqlQuery == "" {
		return nil, fmt.Errorf("invalid input string")
	}

	lexer := cql.NewCqlLexer(antlr.NewInputStream(cqlQuery))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	if p == nil {
		return nil, fmt.Errorf("error while creating parser object")
	}

	if !isDebug {
		p.RemoveErrorListeners()
	}
	return p, nil
}

// buildWhereClause(): takes a slice of Clause structs and returns a string representing the WHERE clause of a bigtable SQL query.
// It iterates over the clauses and constructs the WHERE clause by combining the column name, operator, and value of each clause.
// If the operator is "IN", the value is wrapped with the UNNEST function.
// The constructed WHERE clause is returned as a string.
func buildWhereClause(clauses []Clause, t *Translator, tableName string, keySpace string) (string, error) {
	whereClause := ""
	columnFamily := t.SchemaMappingConfig.SystemColumnFamily
	for _, val := range clauses {
		column := "`" + val.Column + "`"
		value := val.Value
		if colMeta, ok := t.SchemaMappingConfig.TablesMetaData[keySpace][tableName][val.Column]; ok {
			// Check if the column is a primitive type and prepend the column family
			if !colMeta.IsCollection {
				var castErr error
				column, castErr = castColumns(colMeta, columnFamily)
				if castErr != nil {
					return "", castErr
				}
			}
		}
		if whereClause != "" {
			whereClause += " AND "
		}
		if val.Operator == "IN" {
			whereClause += fmt.Sprintf("%s IN UNNEST(%s)", column, val.Value)
		} else {
			whereClause += fmt.Sprintf("%s %s %s", column, val.Operator, value)
		}
	}

	if whereClause != "" {
		whereClause = " WHERE " + whereClause
	}
	return whereClause, nil
}

// castColumns(): constructs the corresponding Bigtable column reference string.
//
// Parameters:
//   - colMeta: Metadata for the column, including its name and type.
//   - columnFamily: The column family in Bigtable.
//
// Returns:
//   - nc: The Bigtable column reference string.
//   - err: An error if the conversion fails.
func castColumns(colMeta *schemaMapping.Column, columnFamily string) (string, error) {
	var nc string
	switch colMeta.ColumnType {
	case "int":
		if colMeta.IsPrimaryKey {
			nc = colMeta.ColumnName
		} else {
			nc = fmt.Sprintf("TO_INT64(%s['%s'])", columnFamily, colMeta.ColumnName)
		}
	case "bigint":
		if colMeta.IsPrimaryKey {
			nc = colMeta.ColumnName
		} else {
			nc = fmt.Sprintf("TO_INT64(%s['%s'])", columnFamily, colMeta.ColumnName)
		}
	case "float":
		nc = fmt.Sprintf("TO_FLOAT32(%s['%s'])", columnFamily, colMeta.ColumnName)
	case "double":
		nc = fmt.Sprintf("TO_FLOAT64(%s['%s'])", columnFamily, colMeta.ColumnName)
	case "boolean":
		nc = fmt.Sprintf("TO_INT64(%s['%s'])", columnFamily, colMeta.ColumnName)
	case "timestamp":
		nc = fmt.Sprintf("TO_TIME(%s['%s'])", columnFamily, colMeta.ColumnName)
	case "blob":
		nc = fmt.Sprintf("TO_BLOB(%s['%s'])", columnFamily, colMeta.ColumnName)
	case "text", "varchar":
		if colMeta.IsPrimaryKey {
			nc = colMeta.ColumnName
		} else {
			nc = fmt.Sprintf("%s['%s']", columnFamily, colMeta.ColumnName)
		}
	default:
		return "", fmt.Errorf("unsupported CQL type: %s", colMeta.ColumnType)
	}
	return nc, nil
}

// parseWhereByClause(): parse Clauses from the Query
//
// Parameters:
//   - input: The Where Spec context from the antlr Parser.
//   - tableName - Table Name
//   - schemaMapping - JSON Config which maintains column and its datatypes info.
//
// Returns: QueryClauses and an error if any.
func parseWhereByClause(input cql.IWhereSpecContext, tableName string, schemaMapping *schemaMapping.SchemaMappingConfig, keyspace string) (*QueryClauses, error) {
	if input == nil {
		return nil, errors.New("no input parameters found for clauses")
	}

	elements := input.RelationElements().AllRelationElement()
	if elements == nil {
		return nil, errors.New("no input parameters found for clauses")
	}

	if len(elements) == 0 {
		return &QueryClauses{}, nil
	}

	var clauses []Clause
	var response QueryClauses
	var paramKeys []string

	params := make(map[string]interface{})

	for i, val := range elements {
		if val == nil {
			return nil, errors.New("could not parse column object")
		}
		s := strconv.Itoa(i + 1)
		placeholder := "value" + s
		operator := ""
		paramKeys = append(paramKeys, placeholder)
		colObj := val.OBJECT_NAME(0)
		if colObj == nil {
			return nil, errors.New("could not parse column object")
		}
		isInOperator := false

		if val.OPERATOR_EQ() != nil {
			operator = val.OPERATOR_EQ().GetText()
		} else if val.OPERATOR_GT() != nil {
			operator = val.OPERATOR_GT().GetSymbol().GetText()
		} else if val.OPERATOR_LT() != nil {
			operator = val.OPERATOR_LT().GetSymbol().GetText()
		} else if val.OPERATOR_GTE() != nil {
			operator = val.OPERATOR_GTE().GetSymbol().GetText()
		} else if val.OPERATOR_LTE() != nil {
			operator = val.OPERATOR_LTE().GetSymbol().GetText()
		} else if val.KwIn() != nil {
			operator = "IN"
			isInOperator = true
		} else {
			return nil, errors.New("no supported operator found")
		}

		colName := colObj.GetText()
		if colName == "" {
			return nil, errors.New("could not parse column name")
		}
		colName = strings.ReplaceAll(colName, literalPlaceholder, "")
		columnType, err := schemaMapping.GetColumnType(keyspace, tableName, colName)
		if err != nil {
			return nil, err
		}
		if columnType != nil && columnType.CQLType != "" {
			if !isInOperator {
				valConst := val.Constant()
				if valConst == nil {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value := val.Constant().GetText()
				if value == "" {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value = strings.ReplaceAll(value, "'", "")

				if value != questionMark {

					val, err := stringToPrimitives(value, columnType.CQLType)
					if err != nil {
						return nil, err
					}

					params[placeholder] = val
				}
			} else {

				lower := strings.ToLower(val.GetText())
				if !strings.Contains(lower, "?") {

					valueFn := val.FunctionArgs()
					if valueFn == nil {
						return nil, errors.New("could not parse Function arguments")
					}
					value := valueFn.AllConstant()
					if value == nil {
						return nil, errors.New("could not parse all values inside IN operator")
					}
					switch columnType.CQLType {

					case "int":
						var allValues []int
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							i, err := strconv.Atoi(valueTxt)
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					case "bigint":
						var allValues []int64
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							i, err := strconv.ParseInt(valueTxt, 10, 64)
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					case "float":
						var allValues []float32
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							i, err := strconv.ParseFloat(valueTxt, 32)

							if err != nil {
								return nil, err
							}
							allValues = append(allValues, float32(i))
						}
						params[placeholder] = allValues
					case "double":
						var allValues []float64
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							i, err := strconv.ParseFloat(valueTxt, 64)
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					case "boolean":
						var allValues []bool
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							val := (valueTxt == "true")
							allValues = append(allValues, val)
						}
						params[placeholder] = allValues
					case "blob":
					case "varchar", "timestamp":
						var allValues []string
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							allValues = append(allValues, valueTxt)
						}
						params[placeholder] = allValues

					default:
						err := errors.New("no correct Datatype found for column")
						return nil, err
					}
				}
			}
			clause := &Clause{
				Column:       colName,
				Operator:     operator,
				Value:        "@" + placeholder,
				IsPrimaryKey: columnType.IsPrimaryKey,
			}
			clauses = append(clauses, *clause)
		}
	}
	response.Clauses = clauses
	response.Params = params
	response.ParamKeys = paramKeys
	return &response, nil
}

// GetColumnFamily(): The function returns the column family for given column, it returns default configured column family for
// primitive columns and the column name as column family for collection type of columns.
func (t *Translator) GetColumnFamily(keyspace, tableName, columnName string) string {
	if colType, err := t.SchemaMappingConfig.GetColumnType(keyspace, tableName, columnName); err == nil {
		if strings.HasPrefix(colType.CQLType, "map<") {
			return columnName
		} else if strings.HasPrefix(colType.CQLType, "set<") {
			return columnName
		} else if strings.HasPrefix(colType.CQLType, "list<") {
			return columnName
		}
	}
	return t.SchemaMappingConfig.SystemColumnFamily
}

// IsMapKey(): Detects if the key is accessing a map element like map[key]
func IsMapKey(key string) bool {
	return strings.Contains(key, "[") && strings.Contains(key, "]")
}

// ExtractMapKey(): Extracts the map name and key from a string like map[key]
func ExtractMapKey(key string) (mapName, mapKey string) {
	openIdx := strings.Index(key, "[")
	closeIdx := strings.LastIndex(key, "]")
	if openIdx == -1 || closeIdx == -1 || closeIdx <= openIdx {
		return key, ""
	}
	mapName = key[:openIdx]
	mapKey = key[openIdx+1 : closeIdx]
	mapKey = strings.ReplaceAll(mapKey, "'", "")
	return mapName, mapKey
}

// ExtractCFAndQualifiersforMap(): Helper function to extract column family and qualifiers from map -{} stirng
func ExtractCFAndQualifiersforMap(value string) (string, []string, error) {
	splits := strings.Split(value, "-")
	if len(splits) < 2 {
		return "", nil, fmt.Errorf("invalid format")
	}
	columnFamily := strings.TrimSpace(splits[0])
	cleanedString := strings.Trim(splits[1], "{}")
	qualifiers := strings.Split(cleanedString, ",")
	if len(qualifiers) == 0 || columnFamily == "" {
		return "", nil, fmt.Errorf("invalid format")
	}
	return columnFamily, qualifiers, nil
}

// getTimestampValue(): getTTLValue parses Value for USING TIMESTAMP
//
// Parameters:
//   - spec: Using TTL TImestamp Spec Context from antlr parser.
//
// Returns: string as timestamp value and error if any
func getTimestampValue(spec cql.IUsingTtlTimestampContext) (string, error) {
	if spec == nil {
		return "", errors.New("invalid input")
	}

	spec.KwUsing()
	tsSpec := spec.Timestamp()

	if tsSpec == nil {
		return questionMark, nil
	}
	tsSpec.KwTimestamp()
	valLiteral := tsSpec.DecimalLiteral()
	var resp string

	if valLiteral == nil {
		return "", errors.New("no value found for Timestamp")
	}

	resp = valLiteral.GetText()
	resp = strings.ReplaceAll(resp, "'", "")
	return resp, nil
}

// GetTimestampInfo(): Extracts timestamp information from an insert query and converts it into a Bigtable timestamp
func GetTimestampInfo(queryStr string, insertObj cql.IInsertContext, index int32) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	lowerQuery := strings.ToLower(queryStr)
	hasUsingTimestamp := hasUsingTimestamp(lowerQuery)
	if hasUsingTimestamp {
		tsSpec := insertObj.UsingTtlTimestamp()
		if tsSpec == nil {
			return timestampInfo, errors.New("error parsing timestamp spec")
		}
		tsValue, err := getTimestampValue(tsSpec)
		if err != nil {
			return timestampInfo, err
		}
		return convertToBigtableTimestamp(tsValue, index)
	}
	return timestampInfo, nil
}

// convertToBigtableTimestamp(): Converts a string timestamp value into a Bigtable timestamp.
func convertToBigtableTimestamp(tsValue string, index int32) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	unixTime := int64(0)
	var err error
	if tsValue != "" && !strings.Contains(tsValue, questionMark) {
		unixTime, err = strconv.ParseInt(tsValue, 10, 64)
		if err != nil {
			return timestampInfo, err
		}
	}
	t := time.Unix(unixTime, 0)
	switch len(tsValue) {
	case 13: // Milliseconds
		t = time.Unix(0, unixTime*int64(time.Millisecond))
	case 16: // Microseconds
		t = time.Unix(0, unixTime*int64(time.Microsecond))
	}
	microsec := t.UnixMicro()
	timestampInfo.Timestamp = bigtable.Timestamp(microsec)
	timestampInfo.HasUsingTimestamp = true
	timestampInfo.Index = index
	return timestampInfo, nil
}

// GetTimestampInfoForRawDelete(): Extracts timestamp information from a raw delete query and converts it into a Bigtable timestamp.
func GetTimestampInfoForRawDelete(queryStr string, deleteObj cql.IDelete_Context) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	lowerQuery := strings.ToLower(queryStr)
	hasUsingTimestamp := hasUsingTimestamp(lowerQuery)
	timestampInfo.HasUsingTimestamp = false
	if hasUsingTimestamp {
		tsSpec := deleteObj.UsingTimestampSpec()
		if tsSpec == nil {
			return timestampInfo, ErrParsingTs
		}

		timestampSpec := tsSpec.Timestamp()
		if timestampSpec == nil {
			return timestampInfo, ErrParsingDelObj
		}

		if tsSpec.KwUsing() != nil {
			tsSpec.KwUsing()
		}
		valLiteral := timestampSpec.DecimalLiteral()
		if valLiteral == nil {
			return timestampInfo, ErrTsNoValue
		}
		tsValue := valLiteral.GetText()
		return convertToBigtableTimestamp(tsValue, 0)
	}
	return timestampInfo, nil
}

// GetTimestampInfoByUpdate(): Extracts timestamp information from an update query and converts it into a Bigtable timestamp.
func GetTimestampInfoByUpdate(queryStr string, updateObj cql.IUpdateContext) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	ttlTimestampObj := updateObj.UsingTtlTimestamp()
	lowerQuery := strings.ToLower(queryStr)
	hasUsingTimestamp := hasUsingTimestamp(lowerQuery)
	if ttlTimestampObj != nil && hasUsingTimestamp {
		tsValue, err := getTimestampValue(updateObj.UsingTtlTimestamp())
		if err != nil {
			return TimestampInfo{}, err
		}
		timestampInfo, err = convertToBigtableTimestamp(tsValue, 0)
		if err != nil {
			return TimestampInfo{}, err
		}
	}
	return timestampInfo, nil
}

// getTimestampInfoForPrepareQuery(): Extracts timestamp information for a prepared query.
func getTimestampInfoForPrepareQuery(values []*primitive.Value, index int32, offset int32) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	timestampInfo.HasUsingTimestamp = true
	if values[index] == nil {
		err := fmt.Errorf("error processing timestamp column in prepare insert")
		return timestampInfo, err
	}
	vBytes := values[index].Contents
	decode, err := proxycore.DecodeType(datatype.Bigint, 4, vBytes)
	if err != nil {
		fmt.Println("error while decoding timestamp column in prepare insert:", err)
		return timestampInfo, err
	}
	timestamp := decode.(int64)
	timestampInfo.Timestamp = bigtable.Timestamp(timestamp)
	return timestampInfo, nil
}

// ProcessTimestamp(): Processes timestamp information for an insert query.
func ProcessTimestamp(st *InsertQueryMapping, values []*primitive.Value) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	timestampInfo.HasUsingTimestamp = false
	if st.TimestampInfo.HasUsingTimestamp {
		return getTimestampInfoForPrepareQuery(values, st.TimestampInfo.Index, 0)
	}
	return timestampInfo, nil
}

// ProcessTimestampByUpdate(): Processes timestamp information for an update query.
func ProcessTimestampByUpdate(st *UpdateQueryMapping, values []*primitive.Value) (TimestampInfo, []*primitive.Value, error) {
	var timestampInfo TimestampInfo
	var err error
	timestampInfo.HasUsingTimestamp = false
	if st.TimestampInfo.HasUsingTimestamp {
		timestampInfo, err = getTimestampInfoForPrepareQuery(values, st.TimestampInfo.Index, 0)
		if err != nil {
			return timestampInfo, values, err
		}
		values = values[1:]
		st.VariableMetadata = st.VariableMetadata[1:]
	}
	return timestampInfo, values, nil
}

// ProcessTimestampByDelete(): Processes timestamp information for a delete query.
func ProcessTimestampByDelete(st *DeleteQueryMapping, values []*primitive.Value) (TimestampInfo, []*primitive.Value, error) {
	var timestampInfo TimestampInfo
	timestampInfo.HasUsingTimestamp = false
	var err error
	if st.TimestampInfo.HasUsingTimestamp {
		timestampInfo, err = getTimestampInfoForPrepareQuery(values, st.TimestampInfo.Index, 1)
		if err != nil {
			return timestampInfo, values, err
		}
		values = values[1:]
	}
	return timestampInfo, values, nil
}

// GetAllColumns(): Retrieves all columns for a table from the schema mapping configuration.
func (t *Translator) GetAllColumns(tableName string, keySpace string) ([]string, string, error) {
	tableData := t.SchemaMappingConfig.TablesMetaData[keySpace][tableName]
	if tableData == nil {
		return nil, "", errors.New("schema mapping not found")
	}
	var columns []string
	for _, value := range tableData {
		if !value.IsCollection {
			columns = append(columns, value.ColumnName)
		}
	}
	return columns, t.SchemaMappingConfig.SystemColumnFamily, nil
}

// processTextMapColumn(): Processes a map column for a CQL query and converts it to the appropriate type.
func processTextMapColumn[V any](
	decodedValue interface{},
	protocolV primitive.ProtocolVersion,
	colFamily string,
	cqlType string,
	newValues *[]interface{},
	newColumns *[]Column,
) error {

	decodedMap := make(map[string]V)
	if pointerMap, ok := decodedValue.(map[*string]*V); ok {
		for k, v := range pointerMap {
			decodedMap[*k] = *v
		}
	} else {
		return fmt.Errorf("decodedValue is not of type map[string]%T", new(V))
	}

	for k, v := range decodedMap {
		c := Column{
			Name:         k,
			ColumnFamily: colFamily,
			CQLType:      cqlType,
		}
		valueStr, err := primitivesToString(v)
		if err != nil {
			return fmt.Errorf("failed to convert value for key '%s': %w", k, err)
		}

		val, _ := formatValues(valueStr, cqlType, protocolV)
		*newValues = append(*newValues, val)
		*newColumns = append(*newColumns, c)
	}
	return nil
}

// processTimestampMapColumn(): Processes a timestamp map column for a CQL query and converts it to the appropriate type.
func processTimestampMapColumn[V any](
	decodedValue interface{},
	protocolV primitive.ProtocolVersion,
	colFamily string,
	cqlType string,
	newValues *[]interface{},
	newColumns *[]Column,
) error {
	decodedMap := make(map[int64]V)
	if pointerMap, ok := decodedValue.(map[time.Time]V); ok {
		for k, v := range pointerMap {
			key := k.UnixMilli()
			decodedMap[key] = v
		}
	}
	for k, v := range decodedMap {
		c := Column{
			Name:         strconv.FormatInt(k, 10),
			ColumnFamily: colFamily,
			CQLType:      cqlType,
		}

		valueStr, err := primitivesToString(v)
		if err != nil {
			return fmt.Errorf("failed to convert value for key '%d': %w", k, err)
		}
		val, _ := formatValues(valueStr, cqlType, protocolV)
		*newValues = append(*newValues, val)
		*newColumns = append(*newColumns, c)
	}
	return nil
}

// processSetColumnGeneric(): Processes a set column for a CQL query and converts it to the appropriate type.
func processSetColumnGeneric[T any](
	decodedValue interface{},
	protocolV primitive.ProtocolVersion,
	colFamily string,
	cqlType string,
	newValues *[]interface{},
	newColumns *[]Column,
) error {

	// Assert the type of the decoded value
	setValue, ok := decodedValue.([]T)
	if !ok {
		return fmt.Errorf("unexpected type %T for set<%T>", decodedValue, new(T))
	}
	// Iterate over the elements in the set
	for _, elem := range setValue {
		valueStr, err := primitivesToString(elem)
		if err != nil {
			return fmt.Errorf("failed to convert element to string: %w", err)
		}
		valInInterface, err := DataConversionInInsertionIfRequired(valueStr, protocolV, cqlType, "string")
		if err != nil {
			return fmt.Errorf("failed to convert element to string: %w", err)
		}
		valueStr = valInInterface.(string)
		c := Column{
			Name:         valueStr,
			ColumnFamily: colFamily,
			CQLType:      cqlType,
		}
		// The value parameter is intentionally empty because the column identifier has the value
		val, _ := formatValues("", "varchar", protocolV)
		*newValues = append(*newValues, val)
		*newColumns = append(*newColumns, c)
	}

	return nil
}

// processListColumnGeneric(): Processes a list column for a CQL query and converts it to the appropriate type.
func processListColumnGeneric[T any](
	decodedValue interface{},
	protocolV primitive.ProtocolVersion,
	colFamily string,
	cqlType string,
	newValues *[]interface{},
	newColumns *[]Column,
	complexMeta *ComplexOperation,
) error {

	// Assert the type of the decoded value
	listValues, ok := decodedValue.([]T)
	if !ok {
		return fmt.Errorf("unexpected type %T for set<%T>", decodedValue, new(T))
	}

	var listDelete [][]byte
	prepend := false
	// Iterate over the elements in the List
	for i, elem := range listValues {
		valueStr, err := primitivesToString(elem)
		if err != nil {
			return fmt.Errorf("failed to convert element to string: %w", err)
		}
		if complexMeta != nil && complexMeta.ListDelete {
			val, _ := formatValues(valueStr, cqlType, protocolV)
			listDelete = append(listDelete, val)
			continue
		}
		if complexMeta != nil {
			prepend = complexMeta.PrependList

		}

		encTime := getEncodedTimestamp(i, len(listValues), prepend)

		c := Column{
			Name:         string(encTime),
			ColumnFamily: colFamily,
			CQLType:      cqlType,
		}
		// The value parameter is intentionally empty because the column identifier has the value
		val, _ := formatValues(valueStr, cqlType, protocolV)
		*newValues = append(*newValues, val)
		*newColumns = append(*newColumns, c)
	}
	if len(listDelete) > 0 {
		complexMeta.ListDeleteValues = listDelete
	}
	return nil
}

// ValidateRequiredPrimaryKeys(): Validates that the primary keys in the required and actual lists match.
func ValidateRequiredPrimaryKeys(requiredKey []string, actualKey []string) bool {
	// Check if the length of slices is the same
	if len(requiredKey) != len(actualKey) {
		return false
	}

	// make a copy of the slices so sort doesn't mutate the original slices which other code probably relies on correct ordering of. Kinda lazy solution but these slices should be small
	requiredKeyTmp := make([]string, len(requiredKey))
	copy(requiredKeyTmp, requiredKey)
	actualKeyTmp := make([]string, len(actualKey))
	copy(actualKeyTmp, actualKey)

	// Sort both slices
	sort.Strings(requiredKeyTmp)
	sort.Strings(actualKeyTmp)

	// Compare the sorted slices element by element
	for i := range requiredKeyTmp {
		if requiredKeyTmp[i] != actualKeyTmp[i] {
			return false
		}
	}

	return true
}

// encodeTimestamp(): Encodes a timestamp value into BigEndian octal format.
func encodeTimestamp(millis int64, nanos int32) []byte {
	buf := make([]byte, 12) // 8 bytes for millis + 4 bytes for nanos
	binary.BigEndian.PutUint64(buf[0:8], uint64(millis))
	binary.BigEndian.PutUint32(buf[8:12], uint32(nanos))
	return buf
}

// getEncodedTimestamp(): Generates an encoded timestamp based on the index and length of the list values
func getEncodedTimestamp(index int, totalLength int, prepend bool) []byte {
	now := time.Now().UnixMilli()
	if prepend {
		now = referenceTime - (now - referenceTime)
	}

	nanos := maxNanos - int32(totalLength) + int32(index)
	return encodeTimestamp(now, nanos)
}

// valExistInArr(): Checks if a given value exists in a slice of strings.
func valExistInArr(arr []string, val string) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

// TransformPrepareQueryForPrependList():  Transforms a prepared query for a prepend list operation.
func TransformPrepareQueryForPrependList(query string) (string, []string) {
	// todo: improve parser to Implement this logic. right now parser is not able to detect prepend operations in list
	// if we dont transform the query then with prepedn list we will get error

	// Initialize variables
	var columns []string

	// Use case-insensitive search for ' SET ' and ' WHERE '
	queryUpper := strings.ToUpper(query)
	setIndex := strings.Index(queryUpper, " SET ")
	whereIndex := strings.Index(queryUpper, " WHERE ")

	// If 'SET' or 'WHERE' clauses are not found, return the original query
	if setIndex == -1 || whereIndex == -1 || setIndex >= whereIndex || !strings.Contains(query, "?") {
		return query, columns
	}

	// Extract parts of the query
	beforeSet := query[:setIndex+5] // Include ' SET '
	setClause := query[setIndex+5 : whereIndex]
	afterWhere := query[whereIndex:] // Include ' WHERE ' and the rest

	// Split the SET clause into individual assignments
	assignments := strings.Split(setClause, ",")
	var newAssignments []string

	// Regular expression to match the prepend pattern
	assignRegex := regexp.MustCompile(`(?i)\s*(\w+)\s*=\s*(['"]?\?['"]?)\s*\+\s*(\w+)\s*`)

	for _, assign := range assignments {
		assign = strings.TrimSpace(assign)
		matches := assignRegex.FindStringSubmatch(assign)

		if matches != nil {
			lhs := matches[1]
			questionMark := matches[2]
			rhsVariable := matches[3]

			// Check if LHS and RHS variable names are the same
			if lhs == rhsVariable {
				// Detected a prepend operation, transform it to append
				newAssign := fmt.Sprintf("%s = %s + %s", lhs, lhs, questionMark)
				newAssignments = append(newAssignments, newAssign)
				columns = append(columns, lhs)
				continue
			}
		}
		// If not a prepend operation, keep the original assignment
		newAssignments = append(newAssignments, assign)
	}

	// If no prepend was found, return the original query
	if len(columns) == 0 {
		return query, columns
	}

	// Reconstruct the modified query
	newSetClause := strings.Join(newAssignments, ", ")
	modifiedQuery := beforeSet + newSetClause + afterWhere

	return modifiedQuery, columns
}

// ExtractKeyValue(): ExtractValue extracts the value between curly braces, trims whitespace, and returns it.
// used in extraction of key in complex update of map and list
func ExtractKeyValue(input string) (string, error) {
	// Check if the input string starts with '{' and ends with '}'
	if len(input) < 2 || input[0] != '{' || input[len(input)-1] != '}' {
		return "", fmt.Errorf("invalid input format: expected '{<value>:?}'")
	}
	// Extract the content between the curly braces
	content := input[1 : len(input)-1]
	// Split the content by ':' to get the value part
	parts := strings.SplitN(content, ":", 2)
	if len(parts) == 0 {
		return "", fmt.Errorf("invalid input format: no value found")
	}
	// Extract the value and remove any whitespace
	value := strings.TrimSpace(parts[0])
	return value, nil
}

// ExtractListType(): ExtractValue extracts the value from a string formatted as "list<value>"
func ExtractListType(s string) (value string) {
	// Extract the value between "list<" and ">"
	value = strings.TrimPrefix(s, "list<")
	value = strings.TrimSuffix(value, ">")
	return
}

// ProcessComplexUpdate(): processes complex updates based on column data and value.
func (t *Translator) ProcessComplexUpdate(columns []Column, values []interface{}, tableName, keyspaceName string, prependColumns []string) (map[string]*ComplexOperation, error) {
	complexMeta := make(map[string]*ComplexOperation)

	for i, column := range columns {
		valueStr, ok := values[i].(string)
		if !ok || !containsOperationIndicators(valueStr, column.Name) {
			continue
		}

		if t.IsCollection(keyspaceName, tableName, column.Name) {
			meta, err := processColumnUpdate(valueStr, column, prependColumns)
			if err != nil {
				return nil, err
			}
			complexMeta[column.Name] = meta
		}
	}

	return complexMeta, nil
}

// containsOperationIndicators(): checks if the value string contains operation indicators.
func containsOperationIndicators(val, columnName string) bool {
	return strings.Contains(val, columnName) && (strings.Contains(val, "+") || strings.Contains(val, "-"))
}

// processColumnUpdate(): handles the logic to update a collection column.
func processColumnUpdate(val string, column Column, prependColumns []string) (*ComplexOperation, error) {
	appendOp, prependOp, listDelete := false, false, false
	var updateListIndex, key string
	var expectedDt datatype.DataType
	var err error

	if strings.Contains(val, "+") {
		if strings.Contains(column.CQLType, "map") {
			key, expectedDt, err = processMapUpdate(val, column)
			appendOp = true
		} else if strings.Contains(column.CQLType, "list") {
			prependOp, appendOp, updateListIndex, expectedDt, err = processListUpdate(val, column, prependColumns)
		}
	} else if strings.Contains(val, "-") {
		if strings.Contains(column.CQLType, "map") {
			expectedDt, err = getMapRemoveDatatype(column)
		} else if strings.Contains(column.CQLType, "list") {
			listDelete = true
		}
	}

	if err != nil {
		return nil, fmt.Errorf("invalid format in prepared complex update: %w", err)
	}

	return &ComplexOperation{
		Append:           appendOp,
		mapKey:           key,
		ExpectedDatatype: expectedDt,
		PrependList:      prependOp,
		UpdateListIndex:  updateListIndex,
		Delete:           strings.Contains(val, "-"),
		ListDelete:       listDelete,
	}, nil
}

// processMapUpdate(): handles map column updates and returns relevant data.
func processMapUpdate(val string, column Column) (string, datatype.DataType, error) {
	if !strings.Contains(val, "{") { // this checks if its a single key update opperation like set map[key]=?
		return "", nil, nil
	}
	splits := strings.Split(val, "+")
	if len(splits) < 2 {
		return "", nil, fmt.Errorf("invalid format")
	}
	key, err := ExtractKeyValue(splits[1])
	if err != nil {
		return "", nil, err
	}
	expectedDt, err := getColumnType("map", column)
	if err != nil {
		return "", nil, err
	}
	return key, expectedDt, nil
}

// getMapRemoveDatatype(): determines the datatype for map removal.
func getMapRemoveDatatype(column Column) (datatype.DataType, error) {
	dts := extractMapTypes(column)
	setStr := fmt.Sprintf("set<%s>", dts[1])
	return utilities.GetCassandraColumnType(setStr)
}

// processListUpdate(): handles list column updates and returns relevant data.
func processListUpdate(val string, column Column, prependColumns []string) (bool, bool, string, datatype.DataType, error) {
	var prependOp, appendOp bool
	var updateIndex string
	var expectedDt datatype.DataType
	var err error

	splits := strings.Split(val, "+")
	if len(splits) < 2 {
		return false, false, "", nil, fmt.Errorf("invalid format")
	}

	if valExistInArr(prependColumns, column.Name) {
		prependOp = true
	} else if strings.Contains(splits[1], "{") {
		updateIndex, err = ExtractKeyValue(splits[1])
		if err != nil {
			return false, false, "", nil, err
		}
		expectedDt, err = getColumnType("list", column)
	} else {
		appendOp = true
	}
	return prependOp, appendOp, updateIndex, expectedDt, err
}

// getColumnType(): extracts types from map or list columns.
func getColumnType(typeName string, column Column) (datatype.DataType, error) {
	dts := extractTypes(typeName, column)
	if len(dts) == 1 {
		return utilities.GetCassandraColumnType(dts[0])
	}
	return utilities.GetCassandraColumnType(dts[1])
}

// extractMapTypes(): splits map types from column CQL type.
func extractMapTypes(column Column) []string {
	dt := strings.Split(column.CQLType, "map")
	cleaned := strings.Trim(dt[1], "<>")
	return strings.Split(cleaned, ",")
}

// extractTypes(): extractTypes splits types from map or list columns.
func extractTypes(typeName string, column Column) []string {
	dt := strings.Split(column.CQLType, typeName)
	cleaned := strings.Trim(dt[1], "<>")
	return strings.Split(cleaned, ",")
}

// ExtractWritetimeValue(): extractValue checks the format of the string and extracts the value-needed part.
func ExtractWritetimeValue(s string) (string, bool) {
	s = strings.ToLower(s)
	// Check if the input starts with "writetime(" and ends with ")"
	if strings.HasPrefix(s, "writetime(") && strings.HasSuffix(s, ")") {
		// Extract the content between "writetime(" and ")"
		value := s[len("writetime(") : len(s)-1]
		return value, true
	}
	// Return empty string and false if the format is incorrect
	return "", false
}

// converts all values to the bigtable row key compatible type
func convertAllValuesToRowKeyType(primaryKeys []schemaMapping.Column, values map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, pmk := range primaryKeys {
		if !pmk.IsPrimaryKey {
			continue
		}

		value, exists := values[pmk.ColumnName]
		if !exists {
			return nil, fmt.Errorf("missing primary key `%s`", pmk.ColumnName)
		}

		cqlType, err := utilities.GetCassandraColumnType(pmk.ColumnType)
		if err != nil {
			return nil, err
		}

		switch cqlType {
		case datatype.Int:
			switch v := value.(type) {
			// bigtable row keys don't support int32 so convert all int32 values to int64
			case int:
				result[pmk.ColumnName] = int64(v)
			case int32:
				result[pmk.ColumnName] = int64(v)
			case int64:
				result[pmk.ColumnName] = v
			case string:
				i, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert Int value %s for key %s", value.(string), pmk.ColumnName)
				}
				result[pmk.ColumnName] = int64(i)
			default:
				return nil, fmt.Errorf("failed to convert %T to Int for key %s", value, pmk.ColumnName)
			}
		case datatype.Bigint:
			switch v := value.(type) {
			case int:
				result[pmk.ColumnName] = int64(v)
			case int32:
				result[pmk.ColumnName] = int64(v)
			case int64:
				result[pmk.ColumnName] = value
			case string:
				i, err := strconv.ParseInt(v, 10, 0)
				if err != nil {
					return nil, fmt.Errorf("failed to convert BigInt value %s for key %s", value.(string), pmk.ColumnName)
				}
				result[pmk.ColumnName] = i
			default:
				return nil, fmt.Errorf("failed to convert %T to BigInt for key %s", value, pmk.ColumnName)
			}
		case datatype.Varchar:
			switch v := value.(type) {
			case string:
				// todo move this validation to all columns not just keys
				if !utf8.Valid([]byte(v)) {
					return nil, fmt.Errorf("invalid utf8 value provided for varchar row key field %s", pmk.ColumnName)
				}
				result[pmk.ColumnName] = v
			default:
				return nil, fmt.Errorf("failed to convert %T to BigInt for key %s", value, pmk.ColumnName)
			}
		case datatype.Blob:
			switch v := value.(type) {
			case string:
				result[pmk.ColumnName] = v
			default:
				return nil, fmt.Errorf("failed to convert %T to BigInt for key %s", value, pmk.ColumnName)
			}
		default:
			return nil, fmt.Errorf("unsupported primary key type %s for key %s", cqlType.String(), pmk.ColumnName)
		}
	}
	return result, nil
}

var kOrderedCodeEmptyField = []byte("\x00\x00")
var kOrderedCodeDelimiter = []byte("\x00\x01")

// todo remove encodeIntValuesWithBigEndian flag when we move to ordered code for ints
func createOrderedCodeKey(primaryKeys []schemaMapping.Column, values map[string]interface{}, encodeIntValuesWithBigEndian bool) ([]byte, error) {
	fixedValues, err := convertAllValuesToRowKeyType(primaryKeys, values)
	if err != nil {
		return nil, err
	}

	var result []byte
	var trailingEmptyFields []byte
	for i, pmk := range primaryKeys {
		if i != pmk.PkPrecedence-1 {
			return nil, fmt.Errorf("wrong order for primary keys")
		}
		value, exists := fixedValues[pmk.ColumnName]
		if !exists {
			return nil, fmt.Errorf("missing primary key `%s`", pmk.ColumnName)
		}

		var orderEncodedField []byte
		var err error
		switch v := value.(type) {
		case int64:
			orderEncodedField, err = encodeInt64Key(v, encodeIntValuesWithBigEndian)
			if err != nil {
				return nil, err
			}
		case string:
			orderEncodedField, err = Append(nil, v)
			if err != nil {
				return nil, err
			}
			// the ordered code library always appends a delimiter to strings, but we have custom delimiter logic so remove it
			orderEncodedField = orderEncodedField[:len(orderEncodedField)-2]
		default:
			return nil, fmt.Errorf("unsupported row key type %T", value)
		}

		// Omit trailing empty fields from the encoding. We achieve this by holding
		// them back in a separate buffer until we hit a non-empty field.
		if len(orderEncodedField) == 0 {
			trailingEmptyFields = append(trailingEmptyFields, kOrderedCodeEmptyField...)
			trailingEmptyFields = append(trailingEmptyFields, kOrderedCodeDelimiter...)
			continue
		}

		if len(result) != 0 {
			result = append(result, kOrderedCodeDelimiter...)
		}

		// Since this field is non-empty, any empty fields we held back are not
		// trailing and should not be omitted. Add them in before appending the
		// latest field. Note that they will correctly end with a delimiter.
		result = append(result, trailingEmptyFields...)
		trailingEmptyFields = nil

		// Finally, append the non-empty field
		result = append(result, orderEncodedField...)
	}

	return result, nil
}

func encodeInt64Key(value int64, encodeIntValuesWithBigEndian bool) ([]byte, error) {
	// todo remove once ordered byte encoding is supported for ints
	// override ordered code value with BigEndian
	if encodeIntValuesWithBigEndian {
		if value < 0 {
			return nil, errors.New("row keys cannot contain negative integer values until ordered byte encoding is supported")
		}

		var b bytes.Buffer
		err := binary.Write(&b, binary.BigEndian, value)
		if err != nil {
			return nil, err
		}

		result, err := Append(nil, b.String())
		if err != nil {
			return nil, err
		}

		return result[:len(result)-2], nil
	}

	return Append(nil, value)
}

func DataConversionInInsertionIfRequired(value interface{}, pv primitive.ProtocolVersion, cqlType string, responseType string) (interface{}, error) {
	switch cqlType {
	case "boolean":
		switch responseType {
		case "string":
			val, err := strconv.ParseBool(value.(string))
			if err != nil {
				return nil, err
			}
			if val {
				return "1", nil
			} else {
				return "0", nil
			}
		default:
			return EncodeBool(value, pv)
		}
	case "bigint":
		switch responseType {
		case "string":
			val, err := strconv.ParseInt(value.(string), 10, 64)
			if err != nil {
				return nil, err
			}
			stringVal := strconv.FormatInt(val, 10)
			return stringVal, nil
		default:
			return EncodeBigInt(value, pv)
		}
	case "int":
		switch responseType {
		case "string":
			val, err := strconv.Atoi(value.(string))
			if err != nil {
				return nil, err
			}
			stringVal := strconv.Itoa(val)
			return stringVal, nil
		default:
			return EncodeInt(value, pv)
		}
	default:
		return value, nil
	}
}

func EncodeBool(value interface{}, pv primitive.ProtocolVersion) ([]byte, error) {
	switch v := value.(type) {
	case string:
		val, err := strconv.ParseBool(v)
		if err != nil {
			return nil, err
		}
		strVal := "0"
		if val {
			strVal = "1"
		}
		intVal, _ := strconv.ParseInt(strVal, 10, 64)
		bd, err := proxycore.EncodeType(datatype.Bigint, pv, intVal)
		return bd, err
	case bool:
		var valInBigint int64
		if v {
			valInBigint = 1
		} else {
			valInBigint = 0
		}
		bd, err := proxycore.EncodeType(datatype.Bigint, pv, valInBigint)
		return bd, err
	case []byte:
		vaInInterface, err := proxycore.DecodeType(datatype.Boolean, pv, v)
		if err != nil {
			return nil, err
		}
		if vaInInterface.(bool) {
			return proxycore.EncodeType(datatype.Bigint, pv, 1)
		} else {
			return proxycore.EncodeType(datatype.Bigint, pv, 0)
		}
	default:
		return nil, fmt.Errorf("unsupported type: %v", value)
	}
}

func EncodeBigInt(value interface{}, pv primitive.ProtocolVersion) ([]byte, error) {
	var intVal int64
	var err error
	switch v := value.(type) {
	case string:
		intVal, err = strconv.ParseInt(v, 10, 64)
	case int32:
		intVal = int64(v)
	case int64:
		intVal = v
	case []byte:
		var decoded interface{}
		if len(v) == 4 {
			decoded, err = proxycore.DecodeType(datatype.Int, pv, v)
			if err != nil {
				return nil, err
			}
			intVal = int64(decoded.(int32))
		} else {
			decoded, err = proxycore.DecodeType(datatype.Bigint, pv, v)
			if err != nil {
				return nil, err
			}
			intVal = decoded.(int64)
		}
	default:
		return nil, fmt.Errorf("unsupported type for bigint conversion: %v", value)
	}
	if err != nil {
		return nil, err
	}
	result, err := proxycore.EncodeType(datatype.Bigint, pv, intVal)
	if err != nil {
		return nil, fmt.Errorf("failed to encode bigint: %w", err)
	}
	return result, err
}

func EncodeInt(value interface{}, pv primitive.ProtocolVersion) ([]byte, error) {
	var int32Val int32
	var err error
	switch v := value.(type) {
	case string:
		intVal, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		int32Val = int32(intVal)
	case int32:
		int32Val = v
	case []byte:
		var decoded interface{}
		decoded, err = proxycore.DecodeType(datatype.Int, pv, v)
		if err != nil {
			return nil, err
		}
		int32Val = decoded.(int32)
	default:
		return nil, fmt.Errorf("unsupported type for int conversion: %v", value)
	}
	if err != nil {
		return nil, err
	}
	result, err := proxycore.EncodeType(datatype.Bigint, pv, int64(int32Val))
	if err != nil {
		return nil, fmt.Errorf("failed to encode int: %w", err)
	}
	return result, err
}

// processDeleteOperationForMapAndSet processes delete operations for map and set column types in Cassandra.
// It decodes the collection values and prepares column deletions.
//
// Parameters:
//   - column: Column represents the column information from Cassandra
//   - meta: ComplexOperation contains metadata about the expected datatype
//   - input: ProcessPrepareCollectionsInput contains protocol version and values to process
//   - i: Index of the value being processed in the input values slice
//   - output: ProcessPrepareCollectionsOutput stores the resulting columns to be deleted
//
// Returns:
//   - error: Returns an error if decoding fails or value conversion fails
//
// The function performs the following steps:
//  1. Gets the expected datatype from meta or derives it from the column CQL type
//  2. Decodes the collection value using the protocol version
//  3. Converts the decoded value to an interface slice
//  4. Converts each value to string and adds it to deletion columns
func processDeleteOperationForMapAndSet(
	column Column,
	meta *ComplexOperation,
	input ProcessPrepareCollectionsInput,
	i int,
	output *ProcessPrepareCollectionsOutput,
) error {
	var err error
	expectedDt := meta.ExpectedDatatype
	if expectedDt == nil {
		expectedDt, err = utilities.GetCassandraColumnType(column.CQLType)
		if err != nil {
			return fmt.Errorf("invalid CQL Type %s err: %w", column.CQLType, err)
		}
	}
	decodedValue, err := collectiondecoder.DecodeCollection(expectedDt, input.ProtocolV, input.Values[i].Contents)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType, err)
	}
	setValue, err := convertToInterfaceSlice(decodedValue)
	if err != nil {
		return err
	}
	for _, v := range setValue {
		setVal, err := primitivesToString(v)
		if err != nil {
			return fmt.Errorf("failed to convert value: %w", err)
		}
		output.DelColumns = append(output.DelColumns, Column{
			Name:         setVal,
			ColumnFamily: column.Name,
		})
	}
	return nil
}

// Helper function to convert decoded value to []interface{}
func convertToInterfaceSlice(decodedValue interface{}) ([]interface{}, error) {
	val := reflect.ValueOf(decodedValue)
	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected a slice type, got %T", decodedValue)
	}

	setValue := make([]interface{}, val.Len())
	for i := 0; i < val.Len(); i++ {
		setValue[i] = val.Index(i).Interface()
	}
	return setValue, nil
}

// processMapKeyAppend handles processing of map key appends in Cassandra column operations.
// It decodes values, converts map keys and values to strings, extracts value types,
// and prepares the data for insertion by appending to the output collections.
//
// Parameters:
//   - column: Column struct containing the column details
//   - meta: ComplexOperation containing expected datatype and map key information
//   - input: ProcessPrepareCollectionsInput containing protocol version and input values
//   - i: Index of the current value being processed
//   - output: ProcessPrepareCollectionsOutput for storing processed columns and values
//
// Returns:
//   - error: Returns an error if any operation fails during processing
func processMapKeyAppend(
	column Column,
	meta *ComplexOperation,
	input ProcessPrepareCollectionsInput,
	i int,
	output *ProcessPrepareCollectionsOutput,
	valueType string,
) error {
	decodedValue, err := proxycore.DecodeType(meta.ExpectedDatatype, input.ProtocolV, input.Values[i].Contents)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType, err)
	}
	setKey, err := primitivesToString(meta.mapKey)
	if err != nil {
		return fmt.Errorf("failed to convert/read key: %w", err)
	}
	setValue, err := primitivesToString(decodedValue)
	if err != nil {
		return fmt.Errorf("failed to convert value for key '%s': %w", setKey, err)
	}
	c := Column{
		Name:         setKey,
		ColumnFamily: column.Name,
		CQLType:      valueType,
	}
	val, err := formatValues(setValue, valueType, input.ProtocolV)
	if err != nil {
		return fmt.Errorf("failed to format value: %w", err)
	}
	output.NewValues = append(output.NewValues, val)
	output.NewColumns = append(output.NewColumns, c)

	return nil
}
