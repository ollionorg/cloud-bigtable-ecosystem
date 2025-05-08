package com.google.cloud.kafka.connect.bigtable.wrappers;

import com.google.api.gax.batching.Batcher;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import javax.annotation.Nonnull;

public class BigtableDataClientWrapper implements IBigtableDataClient {

  private final BigtableDataClient dataClient;

  public BigtableDataClientWrapper(BigtableDataClient dataClient) {
    this.dataClient = dataClient;
  }

  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation mutation) {
    return dataClient.checkAndMutateRow(mutation);
  }

  @Override
  public Batcher<RowMutationEntry, Void> newBulkMutationBatcher(@Nonnull String tableId) {
    return dataClient.newBulkMutationBatcher(tableId);
  }

  @Override
  public void close() {
    dataClient.close();
  }
}
