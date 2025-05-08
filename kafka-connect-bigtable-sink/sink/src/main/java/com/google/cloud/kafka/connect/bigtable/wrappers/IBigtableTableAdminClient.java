package com.google.cloud.kafka.connect.bigtable.wrappers;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import java.util.List;

public interface IBigtableTableAdminClient {

  List<String> listTables();

  ApiFuture<Table> createTableAsync(CreateTableRequest request);

  ApiFuture<Table> getTableAsync(String tableId);

  ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request);

  void close();

}
