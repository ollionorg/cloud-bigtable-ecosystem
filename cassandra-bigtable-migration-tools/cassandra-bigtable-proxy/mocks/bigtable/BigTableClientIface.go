// Code generated by mockery v2.53.3. DO NOT EDIT.

package mocks

import (
	context "context"
	bigtableclient "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	message "github.com/datastax/go-cassandra-native-protocol/message"
	mock "github.com/stretchr/testify/mock"
	responsehandler "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	time "time"
	translator "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translator"
)

// BigTableClientIface is an autogenerated mock type for the BigTableClientIface type
type BigTableClientIface struct {
	mock.Mock
}

// AlterTable provides a mock function with given fields: ctx, data, schemaMappingTableName
func (_m *BigTableClientIface) AlterTable(ctx context.Context, data *translator.AlterTableStatementMap, schemaMappingTableName string) error {
	ret := _m.Called(ctx, data, schemaMappingTableName)

	if len(ret) == 0 {
		panic("no return value specified for AlterTable")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *translator.AlterTableStatementMap, string) error); ok {
		r0 = rf(ctx, data, schemaMappingTableName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ApplyBulkMutation provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *BigTableClientIface) ApplyBulkMutation(_a0 context.Context, _a1 string, _a2 []bigtableclient.MutationData, _a3 string) (bigtableclient.BulkOperationResponse, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for ApplyBulkMutation")
	}

	var r0 bigtableclient.BulkOperationResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, []bigtableclient.MutationData, string) (bigtableclient.BulkOperationResponse, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, []bigtableclient.MutationData, string) bigtableclient.BulkOperationResponse); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		r0 = ret.Get(0).(bigtableclient.BulkOperationResponse)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, []bigtableclient.MutationData, string) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Close provides a mock function with no fields
func (_m *BigTableClientIface) Close() {
	_m.Called()
}

// CreateTable provides a mock function with given fields: ctx, data, schemaMappingTableName
func (_m *BigTableClientIface) CreateTable(ctx context.Context, data *translator.CreateTableStatementMap, schemaMappingTableName string) error {
	ret := _m.Called(ctx, data, schemaMappingTableName)

	if len(ret) == 0 {
		panic("no return value specified for CreateTable")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *translator.CreateTableStatementMap, string) error); ok {
		r0 = rf(ctx, data, schemaMappingTableName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteRowNew provides a mock function with given fields: _a0, _a1
func (_m *BigTableClientIface) DeleteRowNew(_a0 context.Context, _a1 *translator.DeleteQueryMapping) (*message.RowsResult, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for DeleteRowNew")
	}

	var r0 *message.RowsResult
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *translator.DeleteQueryMapping) (*message.RowsResult, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *translator.DeleteQueryMapping) *message.RowsResult); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*message.RowsResult)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *translator.DeleteQueryMapping) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DropTable provides a mock function with given fields: ctx, data, schemaMappingTableName
func (_m *BigTableClientIface) DropTable(ctx context.Context, data *translator.DropTableStatementMap, schemaMappingTableName string) error {
	ret := _m.Called(ctx, data, schemaMappingTableName)

	if len(ret) == 0 {
		panic("no return value specified for DropTable")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *translator.DropTableStatementMap, string) error); ok {
		r0 = rf(ctx, data, schemaMappingTableName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ExecuteBigtableQuery provides a mock function with given fields: _a0, _a1
func (_m *BigTableClientIface) ExecuteBigtableQuery(_a0 context.Context, _a1 responsehandler.QueryMetadata) (map[string]map[string]interface{}, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for ExecuteBigtableQuery")
	}

	var r0 map[string]map[string]interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, responsehandler.QueryMetadata) (map[string]map[string]interface{}, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, responsehandler.QueryMetadata) map[string]map[string]interface{}); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]map[string]interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, responsehandler.QueryMetadata) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetSchemaMappingConfigs provides a mock function with given fields: _a0, _a1, _a2
func (_m *BigTableClientIface) GetSchemaMappingConfigs(_a0 context.Context, _a1 string, _a2 string) (map[string]map[string]*schemaMapping.Column, map[string][]schemaMapping.Column, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for GetSchemaMappingConfigs")
	}

	var r0 map[string]map[string]*schemaMapping.Column
	var r1 map[string][]schemaMapping.Column
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string) (map[string]map[string]*schemaMapping.Column, map[string][]schemaMapping.Column, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string) map[string]map[string]*schemaMapping.Column); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]map[string]*schemaMapping.Column)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string) map[string][]schemaMapping.Column); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string][]schemaMapping.Column)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context, string, string) error); ok {
		r2 = rf(_a0, _a1, _a2)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// InsertRow provides a mock function with given fields: _a0, _a1
func (_m *BigTableClientIface) InsertRow(_a0 context.Context, _a1 *translator.InsertQueryMapping) (*message.RowsResult, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for InsertRow")
	}

	var r0 *message.RowsResult
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *translator.InsertQueryMapping) (*message.RowsResult, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *translator.InsertQueryMapping) *message.RowsResult); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*message.RowsResult)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *translator.InsertQueryMapping) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LoadConfigs provides a mock function with given fields: rh, schemaConfig
func (_m *BigTableClientIface) LoadConfigs(rh *responsehandler.TypeHandler, schemaConfig *schemaMapping.SchemaMappingConfig) {
	_m.Called(rh, schemaConfig)
}

// SelectStatement provides a mock function with given fields: _a0, _a1
func (_m *BigTableClientIface) SelectStatement(_a0 context.Context, _a1 responsehandler.QueryMetadata) (*message.RowsResult, time.Time, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for SelectStatement")
	}

	var r0 *message.RowsResult
	var r1 time.Time
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context, responsehandler.QueryMetadata) (*message.RowsResult, time.Time, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, responsehandler.QueryMetadata) *message.RowsResult); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*message.RowsResult)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, responsehandler.QueryMetadata) time.Time); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Get(1).(time.Time)
	}

	if rf, ok := ret.Get(2).(func(context.Context, responsehandler.QueryMetadata) error); ok {
		r2 = rf(_a0, _a1)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// UpdateRow provides a mock function with given fields: _a0, _a1
func (_m *BigTableClientIface) UpdateRow(_a0 context.Context, _a1 *translator.UpdateQueryMapping) (*message.RowsResult, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for UpdateRow")
	}

	var r0 *message.RowsResult
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *translator.UpdateQueryMapping) (*message.RowsResult, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *translator.UpdateQueryMapping) *message.RowsResult); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*message.RowsResult)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *translator.UpdateQueryMapping) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewBigTableClientIface creates a new instance of BigTableClientIface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewBigTableClientIface(t interface {
	mock.TestingT
	Cleanup(func())
}) *BigTableClientIface {
	mock := &BigTableClientIface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
