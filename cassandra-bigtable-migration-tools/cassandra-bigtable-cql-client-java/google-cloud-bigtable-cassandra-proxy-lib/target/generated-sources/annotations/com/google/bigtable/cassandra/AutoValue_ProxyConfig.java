package com.google.bigtable.cassandra;

import java.util.List;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ProxyConfig extends ProxyConfig {

  private final ProxyConfig.CassandraToBigTableConfig cassandraToBigtableConfigs;

  private final List<ProxyConfig.Listener> listeners;

  private final ProxyConfig.OtelConfig otel;

  private final ProxyConfig.LoggerConfig loggerConfig;

  private AutoValue_ProxyConfig(
      ProxyConfig.CassandraToBigTableConfig cassandraToBigtableConfigs,
      List<ProxyConfig.Listener> listeners,
      ProxyConfig.OtelConfig otel,
      ProxyConfig.LoggerConfig loggerConfig) {
    this.cassandraToBigtableConfigs = cassandraToBigtableConfigs;
    this.listeners = listeners;
    this.otel = otel;
    this.loggerConfig = loggerConfig;
  }

  @Override
  ProxyConfig.CassandraToBigTableConfig getCassandraToBigtableConfigs() {
    return cassandraToBigtableConfigs;
  }

  @Override
  List<ProxyConfig.Listener> getListeners() {
    return listeners;
  }

  @Override
  ProxyConfig.OtelConfig getOtel() {
    return otel;
  }

  @Override
  ProxyConfig.LoggerConfig getLoggerConfig() {
    return loggerConfig;
  }

  @Override
  public String toString() {
    return "ProxyConfig{"
        + "cassandraToBigtableConfigs=" + cassandraToBigtableConfigs + ", "
        + "listeners=" + listeners + ", "
        + "otel=" + otel + ", "
        + "loggerConfig=" + loggerConfig
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ProxyConfig) {
      ProxyConfig that = (ProxyConfig) o;
      return this.cassandraToBigtableConfigs.equals(that.getCassandraToBigtableConfigs())
          && this.listeners.equals(that.getListeners())
          && this.otel.equals(that.getOtel())
          && this.loggerConfig.equals(that.getLoggerConfig());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= cassandraToBigtableConfigs.hashCode();
    h$ *= 1000003;
    h$ ^= listeners.hashCode();
    h$ *= 1000003;
    h$ ^= otel.hashCode();
    h$ *= 1000003;
    h$ ^= loggerConfig.hashCode();
    return h$;
  }

  static final class Builder extends ProxyConfig.Builder {
    private ProxyConfig.CassandraToBigTableConfig cassandraToBigtableConfigs;
    private List<ProxyConfig.Listener> listeners;
    private ProxyConfig.OtelConfig otel;
    private ProxyConfig.LoggerConfig loggerConfig;
    Builder() {
    }
    @Override
    ProxyConfig.Builder setCassandraToBigtableConfigs(ProxyConfig.CassandraToBigTableConfig cassandraToBigtableConfigs) {
      if (cassandraToBigtableConfigs == null) {
        throw new NullPointerException("Null cassandraToBigtableConfigs");
      }
      this.cassandraToBigtableConfigs = cassandraToBigtableConfigs;
      return this;
    }
    @Override
    ProxyConfig.Builder setListeners(List<ProxyConfig.Listener> listeners) {
      if (listeners == null) {
        throw new NullPointerException("Null listeners");
      }
      this.listeners = listeners;
      return this;
    }
    @Override
    ProxyConfig.Builder setOtel(ProxyConfig.OtelConfig otel) {
      if (otel == null) {
        throw new NullPointerException("Null otel");
      }
      this.otel = otel;
      return this;
    }
    @Override
    ProxyConfig.Builder setLoggerConfig(ProxyConfig.LoggerConfig loggerConfig) {
      if (loggerConfig == null) {
        throw new NullPointerException("Null loggerConfig");
      }
      this.loggerConfig = loggerConfig;
      return this;
    }
    @Override
    ProxyConfig build() {
      if (this.cassandraToBigtableConfigs == null
          || this.listeners == null
          || this.otel == null
          || this.loggerConfig == null) {
        StringBuilder missing = new StringBuilder();
        if (this.cassandraToBigtableConfigs == null) {
          missing.append(" cassandraToBigtableConfigs");
        }
        if (this.listeners == null) {
          missing.append(" listeners");
        }
        if (this.otel == null) {
          missing.append(" otel");
        }
        if (this.loggerConfig == null) {
          missing.append(" loggerConfig");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_ProxyConfig(
          this.cassandraToBigtableConfigs,
          this.listeners,
          this.otel,
          this.loggerConfig);
    }
  }

}
