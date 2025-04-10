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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal use only.
 */
class ProxyImpl implements Proxy {

  private static final Duration PROXY_SHUTDOWN_TIMEOUT_SECS = Duration.ofSeconds(5);
  private static final Duration PROXY_STARTUP_TIMEOUT_SECS = Duration.ofSeconds(60);
  private static final String USER_AGENT_PREFIX = "java-cassandra-adapter/";
  private static final String PROJECT_PROPERTIES_FILENAME = "project.properties";
  private static final String LOOPBACK_ADDRESS = "127.0.0.1:%s";
  private static final String PROJECT_VERSION_PROPERTY = "lib.version";
  private static final String PROXY_STARTUP_LOG_MESSAGE = "Starting to serve on listener";
  private static final String DATA_CENTER_ENV_VAR = "DATA_CENTER";
  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyImpl.class);

  private final Path proxyExecutablePath;
  private final Path proxyConfigFilePath;
  private final int proxyPort;

  private Process proxyProcess;
  private boolean isRunning;
  private CompletableFuture<Boolean> isProxyStarted;

  /**
   * @param proxyExecutablePath Path to the proxy executable.
   * @param proxyConfigFilePath Path to the proxy configuration file.
   * @param proxyPort Port number that proxy will listen on
   *
   * <p>Example:
   * <pre>
   * ProxyImpl proxy = new ProxyImpl("path/to/proxy-executable", "path/to/config.yaml", 1234);
   * </pre>
   */
  protected ProxyImpl(Path proxyExecutablePath, Path proxyConfigFilePath, int proxyPort) {
    this.proxyExecutablePath = proxyExecutablePath;
    this.proxyConfigFilePath = proxyConfigFilePath;
    this.proxyPort = proxyPort;
  }

  @Override
  public void start() throws IOException {
    ProcessBuilder pb = createProxyProcessBuilder();
    startProxyProcess(pb);
    isProxyStarted = new CompletableFuture<>();
    startThreadToConsumeProxyOutputStream();
    startThreadToConsumeProxyErrorStream();
    waitForProxyProcessToStartup();
    isRunning = true;
  }

  @Override
  public void stop() {
    if (proxyProcess == null) {
      throw new IllegalStateException("Proxy not started");
    }
    LOGGER.debug("Stopping proxy process...");
    try {
      // Try to stop the proxy with SIGINT. The Go process should trap this.
      proxyProcess.destroy();
      // Wait for graceful shutdown
      if (!proxyProcess.waitFor(PROXY_SHUTDOWN_TIMEOUT_SECS.getSeconds(), TimeUnit.SECONDS)) {
        LOGGER.warn("Proxy process did not stop gracefully, forcing termination.");
        proxyProcess.destroyForcibly(); // Forcefully terminate
      }
    } catch (InterruptedException e) {
      LOGGER.warn(
          "Interrupted while waiting for proxy to stop, forcing termination." + e.getMessage());
      proxyProcess.destroyForcibly();
      Thread.currentThread().interrupt(); // Restore interrupted status
    }
    isRunning = false;
    LOGGER.debug("Proxy process stopped.");
  }

  @Override
  public int getProxyPort() {
    return proxyPort;
  }

  @Override
  public boolean isRunning() {
    return isRunning && proxyPort != 0;
  }

  public String getDatacenter() {
    return BIGTABLE_PROXY_LOCAL_DATACENTER;
  }

  private ProcessBuilder createProxyProcessBuilder() {
    // Build the command to start the proxy
    List<String> command = new ArrayList<>();
    command.add(proxyExecutablePath.toAbsolutePath().toString());

    // Add config file path argument
    if (proxyConfigFilePath == null || proxyConfigFilePath.toAbsolutePath().toString().trim().isEmpty()) {
      throw new IllegalArgumentException("No proxy configuration file specified");
    }
    command.add("-f");
    command.add(proxyConfigFilePath.toAbsolutePath().toString());

    // Add user agent argument
    command.add("-u");
    command.add(USER_AGENT_PREFIX + getLibraryVersion());

    // Add TCP bind port argument
    command.add("-t");
    command.add(LOOPBACK_ADDRESS);

    // Create ProcessBuilder
    ProcessBuilder pb = new ProcessBuilder(command);

    // Set Data Center
    pb.environment().put(DATA_CENTER_ENV_VAR, BIGTABLE_PROXY_LOCAL_DATACENTER);

    return pb;
  }

  private String getLibraryVersion() {
    final Properties properties = new Properties();
    try (InputStream resourceAsStream = ProxyImpl.class.getClassLoader()
        .getResourceAsStream(PROJECT_PROPERTIES_FILENAME)) {
      properties.load(resourceAsStream);
      return properties.getProperty(PROJECT_VERSION_PROPERTY);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get library version", e);
    }
  }

  private void startProxyProcess(ProcessBuilder pb) throws IOException {
    // Start the process
    LOGGER.debug("Starting proxy with command: " + pb.command());
    try {
      proxyProcess = pb.start();

      // Add shutdown hook to stop proxy
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        if (proxyProcess != null && proxyProcess.isAlive()) {
          LOGGER.debug("Shutdown hook: Terminating proxy...");
          stop();
        }
      }));
    } catch (IOException e) {
      throw new IOException("Failed to start proxy process", e);
    }
  }

  private void startThreadToConsumeProxyOutputStream() {
    Thread outputConsumerThread = new Thread(() -> {
      Logger proxyProcessOutputLogger = LoggerFactory.getLogger("Proxy Output");
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(proxyProcess.getInputStream(), StandardCharsets.UTF_8))) {
        String logLine;
        while ((logLine = reader.readLine()) != null) {
          proxyProcessOutputLogger.debug(logLine);
          if (logLine.contains(PROXY_STARTUP_LOG_MESSAGE)) {
            LOGGER.debug("Proxy started on port: " + proxyPort);
            isProxyStarted.complete(true);
          }
        }
      } catch (IOException e) {
        if (e instanceof SocketException && e.getMessage().contains("Socket closed")) {
          LOGGER.debug("Proxy process closed output stream");
        } else {
          LOGGER.error("Error reading proxy output: " + e.getMessage());
        }
        isProxyStarted.complete(false);
      }
    });
    outputConsumerThread.setDaemon(true);  // Make it a daemon thread
    outputConsumerThread.start();
  }

  private void startThreadToConsumeProxyErrorStream() {
    Thread errorOutputConsumerThread = new Thread(() -> {
      Logger proxyProcessErrorLogger = LoggerFactory.getLogger("Proxy Error");
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(proxyProcess.getErrorStream(), StandardCharsets.UTF_8))) {
        String logLine;
        while ((logLine = reader.readLine()) != null) {
          proxyProcessErrorLogger.error(logLine);
        }
      } catch (IOException e) {
        if (e instanceof SocketException && e.getMessage().contains("Socket closed")) {
          LOGGER.debug("Proxy process closed error output stream");
        } else {
          LOGGER.error("Error reading proxy error output: " + e.getMessage());
        }
      }
    });
    errorOutputConsumerThread.setDaemon(true);  // Make it a daemon thread
    errorOutputConsumerThread.start();
  }

  private void waitForProxyProcessToStartup() {
    LOGGER.debug("Waiting for proxy process to start");
    try {
      boolean proxyStarted = isProxyStarted.get(PROXY_STARTUP_TIMEOUT_SECS.getSeconds(), TimeUnit.SECONDS);
      if (!proxyStarted) {
        throw new IllegalStateException("Proxy process failed to start");
      }
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOGGER.error("Exception while waiting for proxy process to start: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

}
