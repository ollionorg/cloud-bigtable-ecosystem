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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	rh "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
)

// Events
const (
	applyingBigtableMutation       = "Applying Insert/Update Mutation"
	bigtableMutationApplied        = "Insert/Update Mutation Applied"
	callingBigtableSQLAPI          = "Calling Bigtable SQL API"
	bigtableSQLAPICallDone         = "Bigtable SQL API Call Done"
	applyingDeleteMutation         = "Applying Delete Mutation"
	deleteMutationApplied          = "Delete Mutation Applied"
	fetchingSchemaMappingConfig    = "Fetching Schema Mapping Configurations"
	schemaMappingConfigFetched     = "Schema Mapping Configurations Fetched"
	applyingBulkMutation           = "Applying Bulk Mutation"
	bulkMutationApplied            = "Bulk Mutation Applied"
	mutationTypeInsert             = "Insert"
	mutationTypeDelete             = "Delete"
	mutationTypeDeleteColumnFamily = "DeleteColumnFamilies"
	mutationTypeUpdate             = "Update"
	schemaMappingTableColumnFamily = "cf"
)

type BigTableClientIface interface {
	ApplyBulkMutation(context.Context, string, []MutationData, string) (BulkOperationResponse, error)
	Close()
	DeleteRowNew(context.Context, *translator.DeleteQueryMapping) (*message.RowsResult, error)
	ExecuteBigtableQuery(context.Context, rh.QueryMetadata) (map[string]map[string]interface{}, error)
	GetSchemaMappingConfigs(context.Context, string, string) (map[string]map[string]*schemaMapping.Column, map[string][]schemaMapping.Column, error)
	InsertRow(context.Context, *translator.InsertQueryMapping) (*message.RowsResult, error)
	SelectStatement(context.Context, rh.QueryMetadata) (*message.RowsResult, time.Time, error)
	AlterTable(ctx context.Context, data *translator.AlterTableStatementMap, schemaMappingTableName string) error
	CreateTable(ctx context.Context, data *translator.CreateTableStatementMap, schemaMappingTableName string) error
	DropTable(ctx context.Context, data *translator.DropTableStatementMap, schemaMappingTableName string) error
	UpdateRow(context.Context, *translator.UpdateQueryMapping) (*message.RowsResult, error)

	// Functions realted to updating the intance properties.
	LoadConfigs(rh *responsehandler.TypeHandler, schemaConfig *schemaMapping.SchemaMappingConfig)
}

// NewBigtableClient - Creates a new instance of BigtableClient.
//
// Parameters:
//   - client: Bigtable client.
//   - logger: Logger instance.
//   - sqlClient: Bigtable SQL client.
//   - config: BigtableConfig configuration object.
//   - responseHandler: TypeHandler for response handling.
//   - grpcConn: grpcConn for calling apis.
//   - schemaMapping: schema mapping for response handling.
//
// Returns:
//   - BigtableClient: New instance of BigtableClient
var NewBigtableClient = func(client map[string]*bigtable.Client, adminClients map[string]*bigtable.AdminClient, logger *zap.Logger, sqlClient btpb.BigtableClient, config BigtableConfig, responseHandler rh.ResponseHandlerIface, grpcConn *grpc.ClientConn, schemaMapping *schemaMapping.SchemaMappingConfig) BigTableClientIface {
	return &BigtableClient{
		Clients:             client,
		AdminClients:        adminClients,
		Logger:              logger,
		SqlClient:           sqlClient,
		BigtableConfig:      config,
		ResponseHandler:     responseHandler,
		SchemaMappingConfig: schemaMapping,
		grpcConn:            grpcConn,
	}
}

func (btc *BigtableClient) reloadSchemaMappings(ctx context.Context, keyspace, schemaMappingTableName string) error {
	tbData, pkData, err := btc.GetSchemaMappingConfigs(ctx, keyspace, schemaMappingTableName)
	if err != nil {
		return fmt.Errorf("error when reloading schema mappings for %s.%s: %w", keyspace, schemaMappingTableName, err)
	}

	if btc.SchemaMappingConfig.TablesMetaData == nil {
		btc.SchemaMappingConfig.TablesMetaData = make(map[string]map[string]map[string]*schemaMapping.Column)
	}
	btc.SchemaMappingConfig.TablesMetaData[keyspace] = tbData

	if btc.SchemaMappingConfig.PkMetadataCache == nil {
		btc.SchemaMappingConfig.PkMetadataCache = make(map[string]map[string][]schemaMapping.Column)
	}
	btc.SchemaMappingConfig.PkMetadataCache[keyspace] = pkData

	return nil
}

// scan the schema mapping table to determine if the table exists
func (btc *BigtableClient) tableSchemaExists(ctx context.Context, client *bigtable.Client, schemaMappingTableName string, tableName string) (bool, error) {
	table := client.Open(schemaMappingTableName)
	exists := false
	err := table.ReadRows(ctx, bigtable.PrefixRange(tableName+"#"), func(row bigtable.Row) bool {
		exists = true
		return false
	}, bigtable.LimitRows(1))
	return exists, err
}

func (btc *BigtableClient) tableResourceExists(ctx context.Context, adminClient *bigtable.AdminClient, table string) (bool, error) {
	_, err := adminClient.TableInfo(ctx, table)
	// todo figure out which error message is for table doesn't exist yet or find better check
	if status.Code(err) == codes.NotFound || status.Code(err) == codes.InvalidArgument {
		return false, nil
	}
	// something went wrong
	if err != nil {
		return false, err
	}
	return true, nil
}

// mutateRow() - Applies mutations to a row in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: Name of the table where the row exists.
//   - rowKey: Row key of the row to mutate.
//   - columns: Columns to mutate.
//   - values: Values to set in the columns.
//   - deleteColumnFamilies: Column families to delete.
//
// Returns:
//   - error: Error if the mutation fails.
func (btc *BigtableClient) mutateRow(ctx context.Context, tableName, rowKey string, columns []translator.Column, values []any, deleteColumnFamilies []string, deleteQualifiers []translator.Column, timestamp bigtable.Timestamp, ifSpec translator.IfSpec, keyspace string, ComplexOperation map[string]*translator.ComplexOperation) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, applyingBigtableMutation)
	mut := bigtable.NewMutation()

	btc.Logger.Info("mutating row", zap.String("key", hex.EncodeToString([]byte(rowKey))))

	client, ok := btc.Clients[keyspace]
	if !ok {
		return nil, fmt.Errorf("invalid keySpace - `%s`", keyspace)
	}

	tbl := client.Open(tableName)
	if timestamp == 0 {
		timestamp = bigtable.Timestamp(bigtable.Now().Time().UnixMicro())
	}

	// Delete column families
	for _, cf := range deleteColumnFamilies {
		mut.DeleteCellsInFamily(cf)
	}

	// Handle complex updates
	for cf, meta := range ComplexOperation {
		if meta.UpdateListIndex != "" {
			index, err := strconv.Atoi(meta.UpdateListIndex)
			if err != nil {
				return nil, err
			}
			reqTimestamp, err := btc.getIndexOpTimestamp(ctx, tableName, rowKey, cf, keyspace, index)
			if err != nil {
				return nil, err
			}
			mut.Set(cf, reqTimestamp, timestamp, meta.Value)
		}
		if meta.ListDelete {
			if err := btc.setMutationforListDelete(ctx, tableName, rowKey, cf, keyspace, meta.ListDeleteValues, mut); err != nil {
				return nil, err
			}
		}
	}

	// Delete specific column qualifiers
	for _, q := range deleteQualifiers {
		mut.DeleteCellsInColumn(q.ColumnFamily, q.Name)
	}

	// Set values for columns
	for i, column := range columns {
		if bv, ok := values[i].([]byte); ok {
			mut.Set(column.ColumnFamily, column.Name, timestamp, bv)
		} else {
			btc.Logger.Error("Value is not of type []byte", zap.String("column", column.Name), zap.Any("value", values[i]))
			return nil, fmt.Errorf("value for column %s is not of type []byte", column.Name)
		}
	}

	if ifSpec.IfExists || ifSpec.IfNotExists {
		predicateFilter := bigtable.CellsPerRowLimitFilter(1)
		matched := true
		conditionalMutation := bigtable.NewCondMutation(predicateFilter, mut, nil)
		if ifSpec.IfNotExists {
			conditionalMutation = bigtable.NewCondMutation(predicateFilter, nil, mut)
		}

		err := tbl.Apply(ctx, rowKey, conditionalMutation, bigtable.GetCondMutationResult(&matched))
		otelgo.AddAnnotation(ctx, bigtableMutationApplied)
		if err != nil {
			return nil, err
		}

		return GenerateAppliedRowsResult(keyspace, tableName, ifSpec.IfExists == matched), nil
	}

	// If no conditions, apply the mutation directly
	err := tbl.Apply(ctx, rowKey, mut)
	otelgo.AddAnnotation(ctx, bigtableMutationApplied)
	if err != nil {
		return nil, err
	}

	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			LastContinuousPage: true,
		},
	}, nil
}

func (btc *BigtableClient) DropTable(ctx context.Context, data *translator.DropTableStatementMap, schemaMappingTableName string) error {
	client, ok := btc.Clients[data.Keyspace]
	if !ok {
		return fmt.Errorf("invalid keyspace `%s`", data.Keyspace)
	}
	adminClient, ok := btc.AdminClients[data.Keyspace]
	if !ok {
		return fmt.Errorf("invalid keyspace `%s`", data.Keyspace)
	}

	// first clean up table from schema mapping table because that's the SoT
	tbl := client.Open(schemaMappingTableName)
	var deleteMuts []*bigtable.Mutation
	var rowKeysToDelete []string
	err := tbl.ReadRows(ctx, bigtable.PrefixRange(data.Table+"#"), func(row bigtable.Row) bool {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		deleteMuts = append(deleteMuts, mut)
		rowKeysToDelete = append(rowKeysToDelete, row.Key())
		return true
	})

	if err != nil {
		return err
	}

	btc.Logger.Info("drop table: deleting schema rows")
	_, err = tbl.ApplyBulk(ctx, rowKeysToDelete, deleteMuts)
	if err != nil {
		return err
	}

	// do a read to check if the table exists to save on admin API write quota
	exists, err := btc.tableResourceExists(ctx, adminClient, data.Table)
	if err != nil {
		return err
	}
	if exists {
		btc.Logger.Info("drop table: deleting bigtable table")
		err = adminClient.DeleteTable(ctx, data.Table)
	}
	if err != nil {
		return err
	}

	// this error behavior is done independently of the table resource existing or not because the schema mapping table is the SoT, not the table resource
	if len(rowKeysToDelete) == 0 && !data.IfExists {
		return fmt.Errorf("cannot delete table %s because it does not exist", data.Table)
	}

	btc.Logger.Info("reloading schema mappings")
	err = btc.reloadSchemaMappings(ctx, data.Keyspace, schemaMappingTableName)
	if err != nil {
		return err
	}

	return nil
}

func (btc *BigtableClient) createSchemaMappingTableMaybe(ctx context.Context, keyspace, schemaMappingTableName string) error {
	btc.Logger.Info("ensuring schema mapping table exists")
	adminClient, ok := btc.AdminClients[keyspace]
	if !ok {
		return fmt.Errorf("invalid keyspace `%s`", keyspace)
	}

	// do a read to check if the table exists to save on admin API write quota
	exists, err := btc.tableResourceExists(ctx, adminClient, schemaMappingTableName)
	if err != nil {
		return err
	}
	if !exists {
		err = adminClient.CreateTable(ctx, schemaMappingTableName)
		if err != nil {
			btc.Logger.Error("failed to create schema mapping table", zap.Error(err))
			return err
		}
	}

	err = adminClient.CreateColumnFamily(ctx, schemaMappingTableName, schemaMappingTableColumnFamily)
	if status.Code(err) == codes.AlreadyExists {
		err = nil
	}
	if err != nil {
		return err
	}

	return nil
}

func (btc *BigtableClient) CreateTable(ctx context.Context, data *translator.CreateTableStatementMap, schemaMappingTableName string) error {
	client, ok := btc.Clients[data.Keyspace]
	if !ok {
		return fmt.Errorf("invalid keyspace `%s`", data.Keyspace)
	}
	adminClient, ok := btc.AdminClients[data.Keyspace]
	if !ok {
		return fmt.Errorf("invalid keyspace `%s`", data.Keyspace)
	}

	exists, err := btc.tableSchemaExists(ctx, client, schemaMappingTableName, data.Table)
	if err != nil {
		return err
	}

	if !exists {
		btc.Logger.Info("updating table schema")
		err = btc.updateTableSchema(ctx, data.Keyspace, schemaMappingTableName, data.Table, data.PrimaryKeys, data.Columns, nil)
		if err != nil {
			return err
		}
	}

	var rowKeySchemaFields []bigtable.StructField
	for _, key := range data.PrimaryKeys {
		part, err := createBigtableRowKeyField(key.Name, data.Columns, btc.BigtableConfig.EncodeIntValuesWithBigEndian)
		if err != nil {
			return err
		}
		rowKeySchemaFields = append(rowKeySchemaFields, part)
	}

	columnFamilies := make(map[string]bigtable.Family)
	for _, col := range data.Columns {
		if utilities.IsCollectionDataType(col.Type) {
			columnFamilies[col.Name] = bigtable.Family{
				GCPolicy: bigtable.MaxVersionsPolicy(1),
			}
		}
	}
	columnFamilies[btc.BigtableConfig.DefaultColumnFamily] = bigtable.Family{
		GCPolicy: bigtable.MaxVersionsPolicy(1),
	}

	btc.Logger.Info("creating bigtable table")
	err = adminClient.CreateTableFromConf(ctx, &bigtable.TableConf{
		TableID:        data.Table,
		ColumnFamilies: columnFamilies,
		RowKeySchema: &bigtable.StructType{
			Fields:   rowKeySchemaFields,
			Encoding: bigtable.StructOrderedCodeBytesEncoding{},
		},
	})
	// ignore already exists errors - the schema mapping table is the SoT
	if status.Code(err) == codes.AlreadyExists {
		err = nil
	}
	if err != nil {
		btc.Logger.Error("failed to create bigtable table", zap.Error(err))
		return err
	}

	if exists && !data.IfNotExists {
		return fmt.Errorf("cannot create table %s becauase it already exists", data.Table)
	}

	btc.Logger.Info("reloading schema mappings")
	err = btc.reloadSchemaMappings(ctx, data.Keyspace, schemaMappingTableName)
	if err != nil {
		return err
	}

	return nil
}

func createBigtableRowKeyField(key string, cols []message.ColumnMetadata, encodeIntValuesWithBigEndian bool) (bigtable.StructField, error) {
	for _, column := range cols {
		if column.Name != key {
			continue
		}

		switch column.Type {
		case datatype.Varchar:
			return bigtable.StructField{FieldName: key, FieldType: bigtable.StringType{Encoding: bigtable.StringUtf8BytesEncoding{}}}, nil
		case datatype.Int, datatype.Bigint, datatype.Timestamp:
			// todo remove once ordered byte encoding is supported for ints
			if encodeIntValuesWithBigEndian {
				return bigtable.StructField{FieldName: key, FieldType: bigtable.Int64Type{Encoding: bigtable.BigEndianBytesEncoding{}}}, nil
			}
			return bigtable.StructField{FieldName: key, FieldType: bigtable.Int64Type{Encoding: bigtable.Int64OrderedCodeBytesEncoding{}}}, nil
		default:
			return bigtable.StructField{}, fmt.Errorf("unhandled row key type %s", column.Type)
		}
	}

	// this should never happen given where this is called
	return bigtable.StructField{}, fmt.Errorf("missing primary key `%s` from columns definition", key)
}

func (btc *BigtableClient) AlterTable(ctx context.Context, data *translator.AlterTableStatementMap, schemaMappingTableName string) error {
	adminClient, ok := btc.AdminClients[data.Keyspace]
	if !ok {
		return fmt.Errorf("invalid keyspace `%s`", data.Keyspace)
	}
	// passing nil in as pmks because we don't have access to them here and because primary keys can't be altered
	err := btc.updateTableSchema(ctx, data.Keyspace, schemaMappingTableName, data.Table, nil, data.AddColumns, data.DropColumns)
	if err != nil {
		return err
	}

	for _, col := range data.AddColumns {
		if utilities.IsCollectionDataType(col.Type) {
			err = adminClient.CreateColumnFamily(ctx, data.Table, col.Name)
			if err != nil {
				return err
			}
		}
	}

	btc.Logger.Info("reloading schema mappings")
	err = btc.reloadSchemaMappings(ctx, data.Keyspace, schemaMappingTableName)
	if err != nil {
		return err
	}

	return nil
}

func (btc *BigtableClient) updateTableSchema(ctx context.Context, keyspace string, schemaMappingTableName string, tableName string, pmks []translator.CreateTablePrimaryKeyConfig, addCols []message.ColumnMetadata, dropCols []string) error {
	client, exists := btc.Clients[keyspace]
	if !exists {
		return fmt.Errorf("invalid keyspace `%s`", keyspace)
	}

	ts := bigtable.Now()
	var muts []*bigtable.Mutation
	var rowKeys []string
	for _, col := range addCols {
		mut := bigtable.NewMutation()
		mut.Set(schemaMappingTableColumnFamily, "ColumnName", ts, []byte(col.Name))
		mut.Set(schemaMappingTableColumnFamily, "ColumnType", ts, []byte(col.Type.String()))
		isCollection := utilities.IsCollectionDataType(col.Type)
		mut.Set(schemaMappingTableColumnFamily, "IsCollection", ts, []byte(strconv.FormatBool(isCollection)))
		pmkIndex := slices.IndexFunc(pmks, func(c translator.CreateTablePrimaryKeyConfig) bool {
			return c.Name == col.Name
		})
		mut.Set(schemaMappingTableColumnFamily, "IsPrimaryKey", ts, []byte(strconv.FormatBool(pmkIndex != -1)))
		if pmkIndex != -1 {
			pmkConfig := pmks[pmkIndex]
			mut.Set(schemaMappingTableColumnFamily, "KeyType", ts, []byte(pmkConfig.KeyType))
		} else {
			// overkill, but overwrite any previous KeyType configs which could exist if the table was recreated with different columns
			mut.Set(schemaMappingTableColumnFamily, "KeyType", ts, []byte(""))
		}
		mut.Set(schemaMappingTableColumnFamily, "PK_Precedence", ts, []byte(strconv.Itoa(pmkIndex+1)))
		mut.Set(schemaMappingTableColumnFamily, "TableName", ts, []byte(tableName))
		muts = append(muts, mut)
		rowKeys = append(rowKeys, tableName+"#"+col.Name)
	}
	// note: we only remove the column from the schema mapping table and don't actually delete any data from the data table
	for _, col := range dropCols {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		muts = append(muts, mut)
		rowKeys = append(rowKeys, tableName+"#"+col)
	}

	btc.Logger.Info("updating schema mapping table")
	table := client.Open(schemaMappingTableName)
	_, err := table.ApplyBulk(ctx, rowKeys, muts)

	if err != nil {
		btc.Logger.Error("update schema mapping table failed")
		return err
	}

	return nil
}

// InsertRow - Inserts a row into the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - data: InsertQueryMapping object containing the table, row key, columns, values, and deleteColumnFamilies.
//
// Returns:
//   - error: Error if the insertion fails.
func (btc *BigtableClient) InsertRow(ctx context.Context, insertQueryData *translator.InsertQueryMapping) (*message.RowsResult, error) {
	return btc.mutateRow(ctx, insertQueryData.Table, insertQueryData.RowKey, insertQueryData.Columns, insertQueryData.Values, insertQueryData.DeleteColumnFamilies, []translator.Column{}, insertQueryData.TimestampInfo.Timestamp, translator.IfSpec{IfNotExists: insertQueryData.IfNotExists}, insertQueryData.Keyspace, nil)
}

// UpdateRow - Updates a row in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - data: UpdateQueryMapping object containing the table, row key, columns, values, and DeleteColumnFamilies.
//
// Returns:
//   - error: Error if the update fails.
func (btc *BigtableClient) UpdateRow(ctx context.Context, updateQueryData *translator.UpdateQueryMapping) (*message.RowsResult, error) {
	return btc.mutateRow(ctx, updateQueryData.Table, updateQueryData.RowKey, updateQueryData.Columns, updateQueryData.Values, updateQueryData.DeleteColumnFamilies, updateQueryData.DeleteColumQualifires, updateQueryData.TimestampInfo.Timestamp, translator.IfSpec{IfExists: updateQueryData.IfExists}, updateQueryData.Keyspace, updateQueryData.ComplexOperation)
}

// DeleteRowNew - Deletes a row in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - queryData: DeleteQueryMapping object containing the table and row key.
//
// Returns:
//   - error: Error if the deletion fails.
func (btc *BigtableClient) DeleteRowNew(ctx context.Context, deleteQueryData *translator.DeleteQueryMapping) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, applyingDeleteMutation)
	client, ok := btc.Clients[deleteQueryData.Keyspace]
	if !ok {
		return nil, fmt.Errorf("invalid keySpace - `%s`", deleteQueryData.Keyspace)
	}
	tbl := client.Open(deleteQueryData.Table)
	mut := bigtable.NewMutation()

	if len(deleteQueryData.SelectedColumns) > 0 {
		for _, v := range deleteQueryData.SelectedColumns {
			if v.ListIndex != "" { //handle list delete Items
				index, err := strconv.Atoi(v.ListIndex)
				if err != nil {
					return nil, err
				}
				reqTimestamp, err := btc.getIndexOpTimestamp(ctx, deleteQueryData.Table, deleteQueryData.RowKey, v.Name, deleteQueryData.Keyspace, index)
				if err != nil {
					return nil, err
				}
				mut.DeleteCellsInColumn(v.Name, reqTimestamp)
			} else if v.MapKey != "" { // Handle map delete Items
				mut.DeleteCellsInColumn(v.Name, v.MapKey)
			}
		}
	} else {
		mut.DeleteRow()
	}
	if deleteQueryData.IfExists {
		predicateFilter := bigtable.CellsPerRowLimitFilter(1)
		conditionalMutation := bigtable.NewCondMutation(predicateFilter, mut, nil)
		matched := true
		if err := tbl.Apply(ctx, deleteQueryData.RowKey, conditionalMutation, bigtable.GetCondMutationResult(&matched)); err != nil {
			return nil, err
		}

		if !matched {
			return GenerateAppliedRowsResult(deleteQueryData.Keyspace, deleteQueryData.Table, false), nil
		} else {
			return GenerateAppliedRowsResult(deleteQueryData.Keyspace, deleteQueryData.Table, true), nil
		}
	} else {
		if err := tbl.Apply(ctx, deleteQueryData.RowKey, mut); err != nil {
			return nil, err
		}
	}
	otelgo.AddAnnotation(ctx, deleteMutationApplied)
	var response = message.RowsResult{
		Metadata: &message.RowsMetadata{
			LastContinuousPage: true,
		},
	}
	return &response, nil
}

// GetSchemaMappingConfigs - Retrieves schema mapping configurations from the specified config table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - schemaMappingTable: Name of the table containing the configuration.
//
// Returns:
//   - map[string]map[string]*schemaMapping.Column: Table metadata.
//   - map[string][]schemaMapping.Column: Primary key metadata.
//   - error: Error if the retrieval fails.
func (btc *BigtableClient) GetSchemaMappingConfigs(ctx context.Context, keyspace, schemaMappingTable string) (map[string]map[string]*schemaMapping.Column, map[string][]schemaMapping.Column, error) {
	// if this is the first time we're getting table configs, ensure the schema mapping table exists
	if btc.SchemaMappingConfig == nil || len(btc.SchemaMappingConfig.TablesMetaData) == 0 {
		err := btc.createSchemaMappingTableMaybe(ctx, keyspace, schemaMappingTable)
		if err != nil {
			return nil, nil, err
		}
	}

	otelgo.AddAnnotation(ctx, fetchingSchemaMappingConfig)
	client, ok := btc.Clients[keyspace]
	if !ok {
		return nil, nil, fmt.Errorf("invalid keySpace - `%s`", keyspace)
	}

	table := client.Open(schemaMappingTable)
	filter := bigtable.LatestNFilter(1)

	tableMetadata := make(map[string]map[string]*schemaMapping.Column)
	pkMetadata := make(map[string][]schemaMapping.Column)

	var readErr error
	err := table.ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		// Extract the row key and column values
		var tableName, columnName, columnType, KeyType string
		var isPrimaryKey, isCollection bool
		var pkPrecedence int

		// Extract column values
		for _, item := range row[schemaMappingTableColumnFamily] {
			switch item.Column {
			case schemaMappingTableColumnFamily + ":TableName":
				tableName = string(item.Value)
			case schemaMappingTableColumnFamily + ":ColumnName":
				columnName = string(item.Value)
			case schemaMappingTableColumnFamily + ":ColumnType":
				columnType = string(item.Value)
			case schemaMappingTableColumnFamily + ":IsPrimaryKey":
				isPrimaryKey = string(item.Value) == "true"
			case schemaMappingTableColumnFamily + ":PK_Precedence":
				pkPrecedence, readErr = strconv.Atoi(string(item.Value))
				if readErr != nil {
					return false
				}
			case schemaMappingTableColumnFamily + ":IsCollection":
				isCollection = string(item.Value) == "true"
			case schemaMappingTableColumnFamily + ":KeyType":
				KeyType = string(item.Value)
			}
		}
		cqlType, err := utilities.GetCassandraColumnType(columnType)
		if err != nil {
			readErr = err
			return false
		}
		columnMetadata := message.ColumnMetadata{
			Keyspace: keyspace,
			Table:    tableName,
			Name:     columnName,
			Type:     cqlType,
		}

		// Create a new column struct
		column := schemaMapping.Column{
			ColumnName:   columnName,
			ColumnType:   columnType,
			IsPrimaryKey: isPrimaryKey,
			PkPrecedence: pkPrecedence,
			IsCollection: isCollection,
			Metadata:     columnMetadata,
			KeyType:      KeyType,
		}

		if _, exists := tableMetadata[tableName]; !exists {
			tableMetadata[tableName] = make(map[string]*schemaMapping.Column)
		}

		if _, exists := pkMetadata[tableName]; !exists {
			var pkSlice []schemaMapping.Column
			pkMetadata[tableName] = pkSlice
		}

		// todo: this index is probably wrong - this index will end up being how the columns for this table are sorted in bigtable (not how they were originally written). This means that if a column is added, it could change the index of other tables if it's name is sorted before existing columns (example: "age" vs. ["name", "id", "email"])
		column.Metadata.Index = int32(len(tableMetadata[tableName]))
		tableMetadata[tableName][column.ColumnName] = &column
		if column.IsPrimaryKey {
			pkSlice := pkMetadata[tableName]
			pkSlice = append(pkSlice, column)
			pkMetadata[tableName] = pkSlice
		}

		return true
	}, bigtable.RowFilter(filter))

	// combine errors for simpler error handling
	if err == nil {
		err = readErr
	}

	if err != nil {
		btc.Logger.Error("Failed to read rows from Bigtable - possible issue with schema_mapping table:", zap.Error(err))
		return nil, nil, err
	}

	otelgo.AddAnnotation(ctx, schemaMappingConfigFetched)
	return tableMetadata, sortPkData(pkMetadata), nil
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
		btc.Logger.Info("mutating row BULK", zap.String("key", hex.EncodeToString([]byte(md.RowKey))))
		if _, exists := rowKeyToMutationMap[md.RowKey]; !exists {
			rowKeyToMutationMap[md.RowKey] = bigtable.NewMutation()
		}
		mut := rowKeyToMutationMap[md.RowKey]
		switch md.MutationType {
		case mutationTypeInsert:
			{
				for _, column := range md.Columns {
					mut.Set(column.ColumnFamily, column.Name, bigtable.Now(), column.Contents)
				}
			}
		case mutationTypeDelete:
			{
				mut.DeleteRow()
			}
		case mutationTypeDeleteColumnFamily:
			{
				mut.DeleteCellsInFamily(md.ColumnFamily)
			}
		case mutationTypeUpdate:
			{
				for _, column := range md.Columns {
					mut.Set(column.ColumnFamily, column.Name, bigtable.Now(), column.Contents)
				}
			}
		default:
			return BulkOperationResponse{
				FailedRows: "",
			}, fmt.Errorf("invalid mutation type `%s`", md.MutationType)
		}
	}
	// create mutations from mutation data
	var mutations []*bigtable.Mutation
	var rowKeys []string

	for key, mutation := range rowKeyToMutationMap {
		mutations = append(mutations, mutation)
		rowKeys = append(rowKeys, key)
	}
	otelgo.AddAnnotation(ctx, applyingBulkMutation)

	client, ok := btc.Clients[keyspace]
	if !ok {
		return BulkOperationResponse{
			FailedRows: "All Rows are failed",
		}, fmt.Errorf("invalid keySpace - `%s`", keyspace)
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
			FailedRows: fmt.Sprintf("failed rowkeys: %v", failedRows),
		}
	} else {
		res = BulkOperationResponse{
			FailedRows: "",
		}
	}
	otelgo.AddAnnotation(ctx, bulkMutationApplied)
	return res, nil
}

// Close() gracefully shuts down the Bigtable client and gRPC connection.
//
// It iterates through all active Bigtable clients and closes them before closing the gRPC connection.
func (btc *BigtableClient) Close() {
	for _, clients := range btc.Clients {
		clients.Close()
	}
	btc.grpcConn.Close()
}

// getIndexOpTimestamp() retrieves the timestamp qualifier for a given list index in a Bigtable row.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: The name of the Bigtable table.
//   - rowKey: The row key to look up.
//   - columnFamily: The column family where the list is stored.
//   - keyspace: The keyspace identifier.
//   - index: The index of the list element for which the timestamp is required.
//
// Returns:
//   - string: The timestamp qualifier if found.
//   - error: An error if the row does not exist, the index is out of bounds, or any other retrieval failure occurs.
func (btc *BigtableClient) getIndexOpTimestamp(ctx context.Context, tableName, rowKey, columnFamily, keyspace string, index int) (string, error) {
	client, ok := btc.Clients[keyspace]
	if !ok {
		return "", fmt.Errorf("invalid keySpace - `%s`", keyspace)
	}
	tbl := client.Open(tableName)
	row, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(bigtable.ChainFilters(
		bigtable.FamilyFilter(columnFamily),
		bigtable.LatestNFilter(1), // this filter is so that we fetch only the latest timestamp value
	)))
	if err != nil {
		btc.Logger.Error("Failed to read row", zap.String("RowKey", rowKey), zap.Error(err))
		return "", err
	}

	if len(row[columnFamily]) <= 0 {
		return "", fmt.Errorf("no values found in list %s", columnFamily)
	}
	for i, item := range row[columnFamily] {
		if i == index {
			splits := strings.Split(item.Column, ":")
			qualifier := splits[1]
			return qualifier, nil
		}
	}
	return "", fmt.Errorf("index %d out of bounds for list size %d", index, len(row[columnFamily]))
}

// setMutationforListDelete() applies deletions to specific list elements in a Bigtable row.
//
// This function identifies and removes the specified elements from a list stored in a column family.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: The name of the Bigtable table.
//   - rowKey: The row key containing the list.
//   - columnFamily: The column family storing the list.
//   - keyspace: The keyspace identifier.
//   - deleteList: A slice of byte arrays representing the values to be deleted from the list.
//   - mut: The Bigtable mutation object to which delete operations will be added.
//
// Returns:
//   - error: An error if the row does not exist or if list elements cannot be deleted.
func (btc *BigtableClient) setMutationforListDelete(ctx context.Context, tableName, rowKey, columnFamily, keyspace string, deleteList [][]byte, mut *bigtable.Mutation) error {
	client, ok := btc.Clients[keyspace]
	if !ok {
		return fmt.Errorf("invalid keySpace - `%s`", keyspace)
	}

	tbl := client.Open(tableName)
	row, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(bigtable.ChainFilters(
		bigtable.FamilyFilter(columnFamily),
		bigtable.LatestNFilter(1), // this filter is so that we fetch only the latest timestamp value
	)))
	if err != nil {
		btc.Logger.Error("Failed to read row", zap.String("RowKey", rowKey), zap.Error(err))
		return err
	}

	if len(row[columnFamily]) <= 0 {
		return fmt.Errorf("no values found in list %s", columnFamily)
	}
	for _, item := range row[columnFamily] {
		for _, dItem := range deleteList {
			if bytes.Equal(dItem, item.Value) {
				splits := strings.Split(item.Column, ":")
				qualifier := splits[1]
				mut.DeleteCellsInColumn(columnFamily, qualifier)
			}
		}
	}
	return nil
}

// LoadConfigs initializes the BigtableClient with the provided response handler
// and schema mapping configuration.
func (btc *BigtableClient) LoadConfigs(rh *responsehandler.TypeHandler, schemaConfig *schemaMapping.SchemaMappingConfig) {
	// Set the ResponseHandler for the BigtableClient to handle responses.
	btc.ResponseHandler = rh

	// Set the SchemaMappingConfig to define how data mapping will be handled in Bigtable.
	btc.SchemaMappingConfig = schemaConfig
}
