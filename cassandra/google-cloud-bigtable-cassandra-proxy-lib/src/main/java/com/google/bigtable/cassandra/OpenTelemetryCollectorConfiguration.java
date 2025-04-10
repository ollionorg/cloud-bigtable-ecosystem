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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import java.util.Optional;

/**
 * Configures the proxy to communicate with an OpenTelemetry collector (<a
 * href="https://opentelemetry.io/docs/collector/">...</a>).
 *
 * <p>This class provides a builder to configure various collector parameters.
 */
@AutoValue
public abstract class OpenTelemetryCollectorConfiguration implements OpenTelemetryConfiguration {

  private static final String DEFAULT_SERVICE_NAME = "cassandra-to-bigtable-otel-service";

  /**
   * @see OpenTelemetryCollectorConfiguration.Builder#setServiceName(String)
   *
   * @return The service name.
   */
  public abstract String serviceName();

  /**
   * @see OpenTelemetryCollectorConfiguration.Builder#setHealthcheckEndpoint(String)
   *
   * @return An {@link Optional} containing the healthcheck endpoint.
   */
  public abstract Optional<String> healthcheckEndpoint();

  /**
   * @see OpenTelemetryCollectorConfiguration.Builder#setMetricsEndpoint
   *
   * @return An {@link Optional} containing the metrics endpoint.
   */
  public abstract Optional<String> metricsEndpoint();

  /**
   * @see OpenTelemetryCollectorConfiguration.Builder#enableTracing(String, double)
   *
   * @return An {@link Optional} containing the traces endpoint.
   */
  public abstract Optional<String> tracesEndpoint();

  /**
   * @see OpenTelemetryCollectorConfiguration.Builder#enableTracing(String, double)
   *
   * @return An {@link Optional} containing the traces sampling ratio.
   */
  public abstract Optional<Double> tracesSamplingRatio();

  /**
   * Creates a new {@link Builder} for {@link OpenTelemetryCollectorConfiguration}.
   * <p>
   * A new builder is created with the default service name {@link #DEFAULT_SERVICE_NAME}.
   *
   * @return A new {@link Builder}.
   */
  public static Builder builder() {
    return new AutoValue_OpenTelemetryCollectorConfiguration.Builder()
        .setServiceName(DEFAULT_SERVICE_NAME);
  }

  /**
   * Builder for {@link OpenTelemetryCollectorConfiguration}.
   */
  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Sets the service name.
     *
     * @param name The service name.
     * @return This {@link Builder} instance.
     */
    public abstract Builder setServiceName(String name);

    protected abstract Builder setHealthcheckEndpoint(Optional<String> endpoint);

    /**
     * Sets the healthcheck endpoint.
     * If not set, health checks will not be reported to the collector.
     *
     * @param endpoint The healthcheck endpoint.
     * @return This {@link Builder} instance.
     */
    public Builder setHealthcheckEndpoint(String endpoint) {
      return this.setHealthcheckEndpoint(Optional.of(endpoint));
    }

    protected abstract Builder setMetricsEndpoint(Optional<String> endpoint);

    /**
     * Enables metrics collection by setting the metrics endpoint.
     * If not set, metrics will not be reported to the collector.
     *
     * @param endpoint The host and port of the OpenTelemetry collector process (e.g., "localhost:4317").
     * @return This {@link Builder} instance.
     */
    public Builder enableMetrics(String endpoint) {
      return this.setMetricsEndpoint(Optional.of(endpoint));
    }

    /**
     * Disables metrics collection.
     *
     * @return This {@link Builder} instance.
     */
    public Builder disableMetrics() {
      return this.setMetricsEndpoint(Optional.empty());
    }

    protected abstract Builder setTracesEndpoint(Optional<String> endpoint);

    protected abstract Builder setTracesSamplingRatio(Optional<Double> ratio);

    /**
     * Enables tracing with the specified endpoint and sampling ratio.
     * If not set, trace data will not be reported to the collector.
     *
     * @param endpoint The traces endpoint.
     * @param sampleRatio The sampling ratio (must be greater than 0, up to 1).
     * @return This {@link Builder} instance.
     * @throws IllegalArgumentException if the sampleRatio is not greater than 0, or greater than 1
     */
    public Builder enableTracing(String endpoint, double sampleRatio) {
      Preconditions.checkArgument(Double.compare(sampleRatio, 0) > 0, "sampleRatio must be > 0, call disableTracing() to disable instead");
      Preconditions.checkArgument(Double.compare(sampleRatio, 1.0) < 1, "sampleRatio must be <= 1");

      return this
          .setTracesEndpoint(Optional.of(endpoint))
          .setTracesSamplingRatio(Optional.of(sampleRatio));
    }

    /**
     * Disables tracing.
     *
     * @return This {@link Builder} instance.
     */
    public Builder disableTracing() {
      return this
          .setTracesEndpoint(Optional.empty())
          .setTracesSamplingRatio(Optional.empty());
    }

    /**
     * Builds the {@link OpenTelemetryCollectorConfiguration}.
     *
     * @return The built {@link OpenTelemetryCollectorConfiguration}.
     */
    public abstract OpenTelemetryCollectorConfiguration build();
  }

}
