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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class OpenTelemetryCollectorConfigurationTest {

  @Test
  public void testOpenTelemetryCollectorConfiguration() {
    OpenTelemetryCollectorConfiguration config = OpenTelemetryCollectorConfiguration.builder()
        .setServiceName("someServiceName")
        .enableMetrics("someMetricsEndpoint")
        .setHealthcheckEndpoint("someHealthcheckEndpoint")
        .enableTracing("someTraceEndpoint", 1.0)
        .build();

    assertEquals("someServiceName", config.serviceName());
    assertEquals("someMetricsEndpoint", config.metricsEndpoint().get());
    assertEquals("someHealthcheckEndpoint", config.healthcheckEndpoint().get());
    assertEquals("someTraceEndpoint", config.tracesEndpoint().get());
    assertEquals(1.0, config.tracesSamplingRatio().get());
  }

  @Test
  public void testOpenTelemetryCollectorConfiguration_NoOptionalFieldsAndDefaultServiceName() {
    OpenTelemetryCollectorConfiguration config = OpenTelemetryCollectorConfiguration.builder().build();

    assertEquals("cassandra-to-bigtable-otel-service", config.serviceName());
    assertFalse(config.metricsEndpoint().isPresent());
    assertFalse(config.healthcheckEndpoint().isPresent());
    assertFalse(config.tracesEndpoint().isPresent());
    assertFalse(config.tracesSamplingRatio().isPresent());
  }

  @Test
  public void testOpenTelemetryCollectorConfiguration_InvalidTraceSamplingRatio() {
    assertThrows(IllegalArgumentException.class,
        () -> OpenTelemetryCollectorConfiguration.builder()
        .setServiceName("someServiceName")
        .enableMetrics("someMetricsEndpoint")
        .setHealthcheckEndpoint("someHealthcheckEndpoint")
        .enableTracing("someTraceEndpoint", 0.0)
        .build());
  }

  @Test
  public void testOpenTelemetryCollectorConfiguration_DisableMetricsAndTracing() {
    OpenTelemetryCollectorConfiguration config = OpenTelemetryCollectorConfiguration.builder()
        .setServiceName("someServiceName")
        .setHealthcheckEndpoint("someHealthcheckEndpoint")
        .disableMetrics()
        .disableTracing()
        .build();

    assertEquals("someServiceName", config.serviceName());
    assertEquals("someHealthcheckEndpoint", config.healthcheckEndpoint().get());
    assertFalse(config.metricsEndpoint().isPresent());
    assertFalse(config.tracesEndpoint().isPresent());
    assertFalse(config.tracesSamplingRatio().isPresent());
  }

}
