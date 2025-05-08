package com.google.cloud.kafka.connect.bigtable.wrappers;

import com.google.api.gax.batching.Batcher;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import javax.annotation.Nonnull;

public interface IBigtableDataClient {

  Boolean checkAndMutateRow(ConditionalRowMutation mutation);

  Batcher<RowMutationEntry, Void> newBulkMutationBatcher(@Nonnull String tableId);

  void close();
}
