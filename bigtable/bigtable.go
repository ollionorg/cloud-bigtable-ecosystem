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

package bigtableclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/grpc"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/datastax/go-cassandra-native-protocol/message"
	otelgo "github.com/ollionorg/cassandra-to-bigtable-proxy/otel"
	rh "github.com/ollionorg/cassandra-to-bigtable-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/tableConfig"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/translator"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/utilities"
	"go.uber.org/zap"
)

// Events
const (
	applyingBigtableMutation = "Applying Insert/Update Mutation"
	bigtableMutationApplied  = "Insert/Update Mutation Applied"
	callingBigtableSQLAPI    = "Calling Bigtable SQL API"
	bigtableSQLAPICallDone   = "Bigtable SQL API Call Done"
	applyingDeleteMutation   = "Applying Delete Mutation"
	deleteMutationApplied    = "Delete Mutation Applied"
	fetchingTableConfig      = "Fetching Table Configurations"
	tableConfigFetched       = "Table Configurations Fetched"
	applyingBulkMutation     = "Applying Bulk Mutation"
	bulkMutationApplied      = "Bulk Mutation Applied"
)

type BigtableClientIface interface {
	InsertRow(ctx context.Context, data *translator.InsertQueryMap) (*message.RowsResult, error)
	UpdateRow(ctx context.Context, data *translator.UpdateQueryMap) (*message.RowsResult, error)
	DeleteAllRows(ctx context.Context, tableName string, keyspace string) error
	DeleteRow(ctx context.Context, data *translator.DeleteQueryMap) (*message.RowsResult, error)
	GetTableConfigs(ctx context.Context, configTableName string, keyspace string) (map[string]map[string]*tableConfig.Column, map[string][]tableConfig.Column, error)
	ApplyBulkMutation(ctx context.Context, tableName string, mutationData []MutationData, keyspace string) (BulkOperationResponse, error)
	SelectStatement(ctx context.Context, query rh.QueryMetadata) (*message.RowsResult, time.Duration, error)
	DeleteRowsUsingTimestamp(ctx context.Context, tableName string, columns []string, rowkey string, columnFamily string, timestamp translator.TimestampInfo, keyspace string) error
	ExecuteBigtableQuery(ctx context.Context, query rh.QueryMetadata) (map[string]map[string]interface{}, error)
	Close()
}

var rowsResult = message.RowsResult{
	Metadata: &message.RowsMetadata{
		LastContinuousPage: true,
	},
}

// NewBigtableClient - Creates a new instance of BigtableClientIface.
//
// Parameters:
//   - client: Bigtable client.
//   - logger: Logger instance.
//   - sqlClient: Bigtable SQL client.
//   - config: BigTableConfig configuration object.
//   - responseHandler: TypeHandler for response handling.
//
// Returns:
//   - BigtableClientIface: New instance of BigtableClientIface.
func NewBigtableClient(client map[string]*bigtable.Client, logger *zap.Logger, sqlClient btpb.BigtableClient, config BigTableConfig, responseHandler rh.ResponseHandlerIface, grpcConn *grpc.ClientConn, tableConfig *tableConfig.TableConfig) BigtableClientIface {
	return &BigtableClient{
		Client:          client,
		Logger:          logger,
		SqlClient:       sqlClient,
		BigTableConfig:  config,
		ResponseHandler: responseHandler,
		TableConfig:     tableConfig,
		grpcConn:        grpcConn,
	}
}

// mutateRow - Applies mutations to a row in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: Name of the table where the row exists.
//   - rowKey: Row key of the row to mutate.
//   - columns: Columns to mutate.
//   - values: Values to set in the columns.
//   - deleteColumnFamily: Column families to delete.
//
// Returns:
//   - error: Error if the mutation fails.
func (btc *BigtableClient) mutateRow(ctx context.Context, tableName, rowKey string, columns []translator.Column, values []interface{}, deleteColumnFamily []string, deleteQualifiers []translator.Column, timestamp bigtable.Timestamp, ifSpec translator.IfSpec, keySpace string) (*message.RowsResult, error) {
	mutations := []*bigtable.Mutation{}

	otelgo.AddAnnotation(ctx, applyingBigtableMutation)
	mut := bigtable.NewMutation()

	client, ok := btc.Client[keySpace]
	if !ok {
		return nil, fmt.Errorf("invalid keySpace")
	}

	tbl := client.Open(tableName)
	if timestamp == 0 {
		timestamp = bigtable.Timestamp(bigtable.Now().Time().UnixMicro())
	}
	if len(deleteColumnFamily) > 0 {
		delMutations, err := deleteAllColumnFamilyCell(ctx, deleteColumnFamily)
		mutations = append(mutations, delMutations)
		if err != nil {
			btc.Logger.Error("Error while Mutation Deleting column family", zap.String("rowKey", rowKey), zap.Error(err))
			return nil, err
		}
	}

	if len(deleteQualifiers) > 0 {
		for _, q := range deleteQualifiers {
			mut.DeleteCellsInColumn(q.ColumnFamily, q.Name)
		}
	}

	for i, column := range columns {
		if bv, ok := values[i].([]byte); ok {
			mut.Set(column.ColumnFamily, column.Name, timestamp, bv)
		} else {
			btc.Logger.Error("Value is not of type []byte", zap.String("column", column.Name), zap.Any("value", values[i]))
			return nil, fmt.Errorf("value for column %s is not of type []byte", column.Name)
		}
	}

	mutations = append(mutations, mut)

	if ifSpec.IfExists || ifSpec.IfNotExists {
		predicateFilter := bigtable.CellsPerRowLimitFilter(1)

		matched := true // to check if condition matched or not
		if ifSpec.IfExists {
			for _, mut := range mutations {
				conditionalMutation := bigtable.NewCondMutation(predicateFilter, mut, nil)

				if err := tbl.Apply(ctx, rowKey, conditionalMutation, bigtable.GetCondMutationResult(&matched)); err != nil {
					return nil, err
				}
				if !matched {
					return GenerateAppliedRowsResult(keySpace, tableName, false), nil
				} else {
					return GenerateAppliedRowsResult(keySpace, tableName, true), nil
				}
			}
		} else if ifSpec.IfNotExists {
			for _, mut := range mutations {

				conditionalMutation := bigtable.NewCondMutation(predicateFilter, nil, mut)
				if err := tbl.Apply(ctx, rowKey, conditionalMutation, bigtable.GetCondMutationResult(&matched)); err != nil {
					return nil, err
				}
				if matched {
					return GenerateAppliedRowsResult(keySpace, tableName, false), nil
				} else {
					return GenerateAppliedRowsResult(keySpace, tableName, true), nil
				}
			}

		}
	} else {
		for _, mut := range mutations {
			if err := tbl.Apply(ctx, rowKey, mut); err != nil {
				return nil, err
			}
		}
	}
	otelgo.AddAnnotation(ctx, bigtableMutationApplied)
	return &rowsResult, nil
}

// InsertRow - Inserts a row into the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - data: InsertQueryMap object containing the table, row key, columns, values, and deleteColumnFamily.
//
// Returns:
//   - error: Error if the insertion fails.
func (btc *BigtableClient) InsertRow(ctx context.Context, data *translator.InsertQueryMap) (*message.RowsResult, error) {
	return btc.mutateRow(ctx, data.Table, data.RowKey, data.Columns, data.Values, data.DeleteColumnFamily, []translator.Column{}, data.TimestampInfo.Timestamp, translator.IfSpec{IfNotExists: data.IfNotExists}, data.Keyspace)

}

// UpdateRow - Updates a row in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - data: UpdateQueryMap object containing the table, row key, columns, values, and deleteColumnFamily.
//
// Returns:
//   - error: Error if the update fails.
func (btc *BigtableClient) UpdateRow(ctx context.Context, data *translator.UpdateQueryMap) (*message.RowsResult, error) {
	return btc.mutateRow(ctx, data.Table, data.RowKey, data.Columns, data.Values, data.DeleteColumnFamily, data.DeleteColumQualifires, data.TimestampInfo.Timestamp, translator.IfSpec{IfExists: data.IfExists}, data.Keyspace)
}

// DeleteRow - Deletes a row in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - data: DeleteQueryMap object containing the table and row key.
//
// Returns:
//   - error: Error if the deletion fails.
func (btc *BigtableClient) DeleteRow(ctx context.Context, data *translator.DeleteQueryMap) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, applyingDeleteMutation)
	client, ok := btc.Client[data.Keyspace]
	if !ok {
		return nil, fmt.Errorf("invalid keySpace")
	}
	tbl := client.Open(data.Table)
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	if data.IfExists {
		predicateFilter := bigtable.CellsPerRowLimitFilter(1)
		conditionalMutation := bigtable.NewCondMutation(predicateFilter, mut, nil)
		matched := true
		if err := tbl.Apply(ctx, data.RowKey, conditionalMutation, bigtable.GetCondMutationResult(&matched)); err != nil {
			return nil, err
		}

		if !matched {
			return GenerateAppliedRowsResult(data.Keyspace, data.Table, false), nil
		} else {
			return GenerateAppliedRowsResult(data.Keyspace, data.Table, true), nil
		}
	} else {
		if err := tbl.Apply(ctx, data.RowKey, mut); err != nil {
			return nil, err
		}
	}
	otelgo.AddAnnotation(ctx, deleteMutationApplied)
	return &rowsResult, nil
}

// GetTableConfigs - Retrieves table configurations from the specified config table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - configTableName: Name of the table containing the configuration.
//
// Returns:
//   - map[string]map[string]*tableConfig.Column: Table metadata.
//   - map[string][]tableConfig.Column: Primary key metadata.
//   - error: Error if the retrieval fails.
func (btc *BigtableClient) GetTableConfigs(ctx context.Context, configTableName, instanceId string) (map[string]map[string]*tableConfig.Column, map[string][]tableConfig.Column, error) {
	otelgo.AddAnnotation(ctx, fetchingTableConfig)

	client, ok := btc.Client[instanceId]
	if !ok {
		return nil, nil, fmt.Errorf("invalid keySpace")
	}

	table := client.Open(configTableName)
	filter := bigtable.LatestNFilter(1)

	tableMetadata := make(map[string]map[string]*tableConfig.Column)
	pkMetadata := make(map[string][]tableConfig.Column)

	err := table.ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		// Extract the row key and column values
		var tableName, columnName, columnType string
		var isPrimaryKey, isCollection bool
		var pkPrecedence int64

		// Extract column values
		for _, item := range row["cf"] {
			switch item.Column {
			case "cf:TableName":
				tableName = string(item.Value)
			case "cf:ColumnName":
				columnName = string(item.Value)
			case "cf:ColumnType":
				columnType = string(item.Value)
			case "cf:IsPrimaryKey":
				isPrimaryKey = string(item.Value) == "true"
			case "cf:PK_Precedence":
				pkPrecedence, _ = json.Number(string(item.Value)).Int64()
			case "cf:IsCollection":
				isCollection = string(item.Value) == "true"
			}
		}
		cqlType, err := utilities.GetCassandraColumnType(columnType)
		if err != nil {
			btc.Logger.Error("Failed to read row from Bigtable", zap.Error(err))
			return false
		}
		columnMetadata := message.ColumnMetadata{
			Keyspace: "",
			Table:    tableName,
			Name:     columnName,
			Type:     cqlType,
		}

		// Create a new column struct
		column := tableConfig.Column{
			ColumnName:   columnName,
			ColumnType:   columnType,
			IsPrimaryKey: isPrimaryKey,
			PkPrecedence: pkPrecedence,
			IsCollection: isCollection,
			Metadata:     columnMetadata,
		}

		if _, exists := tableMetadata[tableName]; !exists {
			tableMetadata[tableName] = make(map[string]*tableConfig.Column)
		}

		index := len(tableMetadata[tableName])

		if _, exists := pkMetadata[tableName]; !exists {
			var pkSlice []tableConfig.Column
			pkMetadata[tableName] = pkSlice
		}

		column.Metadata.Index = int32(index)
		tableMetadata[tableName][column.ColumnName] = &column
		if column.IsPrimaryKey {
			pkSlice := pkMetadata[tableName]
			pkSlice = append(pkSlice, column)
			pkMetadata[tableName] = pkSlice
		}

		return true
	}, bigtable.RowFilter(filter))

	if err != nil {
		btc.Logger.Error("Failed to read rows from Bigtable", zap.Error(err))
		return nil, nil, err
	}
	if len(tableMetadata) < 1 {
		return nil, nil, fmt.Errorf("no rows for the table config found")
	}
	otelgo.AddAnnotation(ctx, tableConfigFetched)
	return tableMetadata, sortpkData(pkMetadata), nil
}

// deleteAllColumnFamilyCell - Deletes all cells in the specified column families.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - deleteCF: Column families to delete cells from.
//   - rowkey: Row key of the row to delete cells from.
//   - mut: Mutation to apply.
//   - tbl: Bigtable table instance.
//
// Returns:
//   - error: Error if the deletion fails.
func deleteAllColumnFamilyCell(ctx context.Context, deleteCF []string) (*bigtable.Mutation, error) {
	mut := bigtable.NewMutation()
	for _, v := range deleteCF {
		mut.DeleteCellsInFamily(v)
	}
	return mut, nil
}

// DeleteAllRows - Deletes all rows in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: Name of the table to delete all rows from.
//
// Returns:
//   - error: Error if the deletion fails.
func (btc *BigtableClient) DeleteAllRows(ctx context.Context, tableName, keyspace string) error {

	client, ok := btc.Client[keyspace]
	if !ok {
		return fmt.Errorf("invalid keySpace")
	}

	tbl := client.Open(tableName)

	// Use an empty ReadOption array to fetch all rows (only keys are needed, no values)
	var allKeys []string
	err := tbl.ReadRows(ctx, bigtable.PrefixRange(""), func(row bigtable.Row) bool {
		allKeys = append(allKeys, row.Key())
		return true // Continue reading
	}, bigtable.RowFilter(bigtable.StripValueFilter()))

	if err != nil {
		btc.Logger.Error("Failed to read rows for deletion", zap.Error(err))
		return err
	}

	// Now delete all rows that were fetched
	for _, key := range allKeys {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		if err := tbl.Apply(ctx, key, mut); err != nil {
			btc.Logger.Error("Failed to delete row", zap.String("RowKey", key), zap.Error(err))
			// Optionally, continue deleting other rows even if some deletions fail
		}
	}
	return nil
}

// DeleteRowsusingTimestamp - Deletes all rows in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: Name of the table to delete all rows from.
//
// Returns:
//   - error: Error if the deletion fails.
func (btc *BigtableClient) DeleteRowsUsingTimestamp(ctx context.Context, tableName string, columns []string, rowkey string, columnFamily string, timestamp translator.TimestampInfo, keyspace string) error {
	client, ok := btc.Client[keyspace]
	if !ok {
		return fmt.Errorf("invalid keySpace")
	}

	tbl := client.Open(tableName)
	mut := bigtable.NewMutation()
	for _, value := range columns {
		mut.DeleteTimestampRange(columnFamily, value, 0, timestamp.Timestamp)
	}
	if err := tbl.Apply(ctx, rowkey, mut); err != nil {
		btc.Logger.Error("Failed to delete row", zap.String("RowKey", rowkey), zap.Error(err))
	}
	return nil
}

// ApplyBulkMutation - Applies bulk mutations to the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: Name of the table to apply bulk mutations to.
//   - mutationData: Slice of MutationData objects containing mutation details.
//
// Returns:
//   - BulkOperationResponse: Response indicating the result of the bulk operation.
//   - error: Error if the bulk mutation fails.
func (btc *BigtableClient) ApplyBulkMutation(ctx context.Context, tableName string, mutationData []MutationData, keyspace string) (BulkOperationResponse, error) {

	rowKeyToMutationMap := make(map[string]*bigtable.Mutation)
	for _, md := range mutationData {
		if _, exists := rowKeyToMutationMap[md.MutationRowKey]; !exists {
			rowKeyToMutationMap[md.MutationRowKey] = bigtable.NewMutation()
		}
		mut := rowKeyToMutationMap[md.MutationRowKey]
		switch md.MutationType {
		case "Insert":
			{
				for _, column := range md.MutationColumn {
					mut.Set(column.ColumnFamily, column.Name, bigtable.Now(), column.Contents)
				}
			}
		case "Delete":
			{
				mut.DeleteRow()

			}
		case "DeleteColumnFamily":
			{
				mut.DeleteCellsInFamily(md.ColumnFamily)
			}
		case "Update":
			{
				for _, column := range md.MutationColumn {
					mut.Set(column.ColumnFamily, column.Name, bigtable.Now(), column.Contents)
				}
			}
		}
	}
	// create mutations from mutation data
	var mutations []*bigtable.Mutation
	var rowKeys []string

	for key := range rowKeyToMutationMap {
		mutations = append(mutations, rowKeyToMutationMap[key])
		rowKeys = append(rowKeys, key)
	}
	otelgo.AddAnnotation(ctx, applyingBulkMutation)

	client, ok := btc.Client[keyspace]
	if !ok {
		return BulkOperationResponse{
			FailedRows: "All Rows are failed",
		}, fmt.Errorf("invalid keySpace")
	}

	tbl := client.Open(tableName)
	errs, err := tbl.ApplyBulk(ctx, rowKeys, mutations)

	if err != nil {
		return BulkOperationResponse{
			FailedRows: "All Rows are failed",
		}, fmt.Errorf("ApplyBulk: %w", err)
	}

	var failedRows []string
	for i, e := range errs {
		if e != nil {
			failedRows = append(failedRows, rowKeys[i])
		}
	}
	var res BulkOperationResponse
	if len(failedRows) > 0 {
		res = BulkOperationResponse{
			FailedRows: fmt.Sprintf("Failed Row Ids: %v", failedRows),
		}
	} else {
		res = BulkOperationResponse{
			FailedRows: "",
		}
	}
	otelgo.AddAnnotation(ctx, bulkMutationApplied)
	return res, nil
}

func (btc *BigtableClient) Close() {
	for _, clients := range btc.Client {
		clients.Close()
	}
	btc.grpcConn.Close()
}
