// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.bigtable.cassandra;

import com.google.bigtable.cassandra.ProxyConfig.Bigtable;
import com.google.bigtable.cassandra.ProxyConfig.CassandraToBigTableConfig;
import com.google.bigtable.cassandra.ProxyConfig.HealthCheck;
import com.google.bigtable.cassandra.ProxyConfig.Listener;
import com.google.bigtable.cassandra.ProxyConfig.ListenerOtel;
import com.google.bigtable.cassandra.ProxyConfig.LoggerConfig;
import com.google.bigtable.cassandra.ProxyConfig.Metrics;
import com.google.bigtable.cassandra.ProxyConfig.OtelConfig;
import com.google.bigtable.cassandra.ProxyConfig.Session;
import com.google.bigtable.cassandra.ProxyConfig.Traces;
import java.util.ArrayList;
import java.util.List;

/**
 * Internal use only.
 */
class ProxyConfigUtils {

  static protected ProxyConfig createProxyConfig(BigtableCqlConfiguration bigtableCqlConfiguration,
      int proxyPort) {
    CassandraToBigTableConfig cassandraToBigTableConfig = createCassandraToBigTableConfig(
        bigtableCqlConfiguration);
    LoggerConfig loggerConfig = createLoggerConfig();
    List<Listener> listeners = createListeners(bigtableCqlConfiguration, proxyPort);
    OtelConfig otel = createOtelConfig(bigtableCqlConfiguration);

    return ProxyConfig.builder()
        .setCassandraToBigtableConfigs(cassandraToBigTableConfig)
        .setListeners(listeners)
        .setOtel(otel)
        .setLoggerConfig(loggerConfig)
        .build();
  }

  static private CassandraToBigTableConfig createCassandraToBigTableConfig(
      BigtableCqlConfiguration bigtableCqlConfiguration) {
    return CassandraToBigTableConfig.builder()
        .setProjectId(bigtableCqlConfiguration.getProjectId())
        .setSchemaMappingTable(bigtableCqlConfiguration.getSchemaMappingTable()
            .orElse(BigtableCqlConfiguration.DEFAULT_SCHEMA_MAPPING_TABLE))
        .setDefaultColumnFamily(bigtableCqlConfiguration.getDefaultColumnFamily()
            .orElse(BigtableCqlConfiguration.DEFAULT_COLUMN_FAMILY))
        .build();
  }

  static private LoggerConfig createLoggerConfig() {
    return new LoggerConfig(ProxyConfig.LOGGER_STDOUT);
  }

  static private List<Listener> createListeners(BigtableCqlConfiguration bigtableCqlConfiguration,
      int proxyPort) {
    Listener e = Listener.builder()
        .name(ProxyConfig.DEFAULT_LISTENER_NAME)
        .port(proxyPort)
        .bigtable(Bigtable.builder()
            .projectId(bigtableCqlConfiguration.getProjectId())
            .instanceIds(bigtableCqlConfiguration.getInstanceId())
            .schemaMappingTable(bigtableCqlConfiguration.getSchemaMappingTable()
                .orElse(BigtableCqlConfiguration.DEFAULT_SCHEMA_MAPPING_TABLE))
            .appProfileId(bigtableCqlConfiguration.getAppProfileId()
                .orElse(BigtableCqlConfiguration.DEFAULT_APP_PROFILE_ID))
            .defaultColumnFamily(
                bigtableCqlConfiguration.getDefaultColumnFamily()
                    .orElse(BigtableCqlConfiguration.DEFAULT_COLUMN_FAMILY))
            .session(new Session(bigtableCqlConfiguration.getBigtableChannelPoolSize()
                .orElse(BigtableCqlConfiguration.DEFAULT_BIGTABLE_CHANNEL_POOL_SIZE)))
            .build())
        .otel(
            new ListenerOtel(!bigtableCqlConfiguration.getOpenTelemetryConfiguration().isPresent()))
        .build();
    List<Listener> listeners = new ArrayList<>();
    listeners.add(e);
    return listeners;
  }

  static private OtelConfig createOtelConfig(BigtableCqlConfiguration bigtableCqlConfiguration) {
    if (!bigtableCqlConfiguration.getOpenTelemetryConfiguration().isPresent()) {
      return OtelConfig.builder().enabled(false).build();
    }

    OtelConfig.Builder otelBuilder = OtelConfig.builder()
        .enabled(true)
        .enabledClientSideMetrics(true);

    OpenTelemetryCollectorConfiguration openTelemetryConfiguration = (OpenTelemetryCollectorConfiguration) bigtableCqlConfiguration
        .getOpenTelemetryConfiguration()
        .get();

    otelBuilder.serviceName(openTelemetryConfiguration.serviceName());

    openTelemetryConfiguration.metricsEndpoint()
        .ifPresent(me -> otelBuilder.metrics(new Metrics(me)));

    openTelemetryConfiguration.healthcheckEndpoint().ifPresent(he ->
        otelBuilder.healthCheck(HealthCheck.builder()
            .enabled(true)
            .endpoint(he).build()));

    if (openTelemetryConfiguration.tracesEndpoint().isPresent()
        && openTelemetryConfiguration.tracesSamplingRatio().isPresent()) {
      Traces traces = Traces.builder()
          .endpoint(openTelemetryConfiguration.tracesEndpoint().get())
          .samplingRatio(openTelemetryConfiguration.tracesSamplingRatio().get())
          .build();
      otelBuilder.traces(traces);
    }

    return otelBuilder.build();
  }

}
