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

import com.google.common.base.Preconditions;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal use only.
 */
class ProxyManager {

  private static final String PROXY_BINARY_NAME = "cassandra-to-bigtable-proxy";
  private static final String PROXY_TEMP_DIR_PREFIX = "bigtable_cassandra_proxy_";
  private static final String WINDOWS_EXE_EXTENSION = ".exe";
  private static final String PROXY_CONFIG_FILENAME = "config.yaml";
  private static final int UNSPECIFIED_PORT = 0;
  private static final String PROXY_LOCAL_HOSTNAME = "localhost";
  private static final String PERMS_700 = "rwx------";

  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyManager.class);

  private final BigtableCqlConfiguration bigtableCqlConfiguration;

  private Proxy proxy;
  private boolean isProxySetup;

  // Internal use only
  ProxyManager(BigtableCqlConfiguration bigtableCqlConfiguration) {
    Preconditions.checkNotNull(bigtableCqlConfiguration, "Valid BigtableCqlConfig must be supplied");
    this.bigtableCqlConfiguration = bigtableCqlConfiguration;
    this.isProxySetup = false;
  }

  // Internal use only
  ProxyManager(Proxy proxy) {
    Preconditions.checkNotNull(proxy, "Valid Proxy must be supplied");
    this.bigtableCqlConfiguration = null;
    this.proxy = proxy;
    this.isProxySetup = true;
  }

  public void setupProxy() throws IOException {
    Preconditions.checkState(!isProxySetup, "Proxy already setup");

    // TODO: handle potential race between finding a free port and another process taking it
    int proxyPort = findFreeProxyPort();

    // Create temp directory
    Path tempProxyDir = Files.createTempDirectory(PROXY_TEMP_DIR_PREFIX);
    LOGGER.debug("Proxy temp directory: " + tempProxyDir.toAbsolutePath());
    tempProxyDir.toFile().deleteOnExit();

    // Copy proxy binary to temp directory
    Path proxyExecutablePath = copyProxyBinaryToTempDir(tempProxyDir);

    // Write proxy config to temp directory
    ProxyConfig proxyConfig = ProxyConfigUtils.createProxyConfig(bigtableCqlConfiguration, proxyPort);
    Path proxyConfigFilePath = createProxyConfigFile(proxyConfig, tempProxyDir);

    this.proxy = new ProxyImpl(proxyExecutablePath, proxyConfigFilePath, proxyPort);
    isProxySetup = true;
  }

  public void start() throws IOException {
    if (!isProxySetup) {
      throw new IllegalStateException("Must call initializePort and setupProxy first");
    }
    proxy.start();
  }

  public int getProxyPort() {
    if (proxy.getProxyPort() == 0) {
      throw new IllegalStateException(
          "Proxy port not assigned yet. Call initializePort() and start() first.");
    }
    return proxy.getProxyPort();
  }

  public void stop() {
    if (!proxy.isRunning()) {
      throw new IllegalStateException("Proxy not started");
    }
    proxy.stop();
  }

  protected SocketAddress runProxy() throws IOException {
    LOGGER.debug("Setting up proxy");
    setupProxy();
    LOGGER.debug("Starting proxy");
    start();
    int proxyPort = getProxyPort();
    return new InetSocketAddress(PROXY_LOCAL_HOSTNAME, proxyPort);
  }

  private int findFreeProxyPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(UNSPECIFIED_PORT)) {
      if (socket.getLocalPort() == 0) {
        throw new IOException("Could not find a free port for the proxy");
      }
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IOException("Could not find a free port for the proxy", e);
    }
  }

  private Path createProxyConfigFile(ProxyConfig proxyConfig, Path tempProxyDir)
      throws IOException {
    // Serialize proxy config to YAML
    String proxyConfigContents = proxyConfig.toYaml();

    // Write proxy config to temp dir
    Path configTempPath = tempProxyDir.resolve(PROXY_CONFIG_FILENAME);
    Files.createFile(configTempPath);
    configTempPath.toFile().deleteOnExit();

    String proxyConfigFilePath = configTempPath.toAbsolutePath().toString();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(proxyConfigFilePath))) {
      writer.write(proxyConfigContents);
      LOGGER.debug("Successfully wrote proxy config to " + proxyConfigFilePath);
      return configTempPath;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to write proxy config", e);
    }
  }

  private Path copyProxyBinaryToTempDir(Path tempProxyDir) throws IOException {
    // Copy proxy binary from resources
    try (InputStream inputStream = ProxyManager.class.getClassLoader()
        .getResourceAsStream(PROXY_BINARY_NAME)) {
      if (inputStream == null) {
        throw new IOException("Proxy binary not found. Expected to find: " + PROXY_BINARY_NAME);
      }

      // Determine OS-specific binary name
      String os = System.getProperty("os.name").trim().toLowerCase();
      boolean isWindows = os.contains("win");
      String fullProxyBinaryName =
          isWindows ? PROXY_BINARY_NAME + WINDOWS_EXE_EXTENSION : PROXY_BINARY_NAME;

      // Copy proxy binary to temp directory
      Path targetProxyBinaryPath = tempProxyDir.resolve(fullProxyBinaryName);
      Files.copy(inputStream, targetProxyBinaryPath);
      targetProxyBinaryPath.toFile().deleteOnExit();

      // Set file permissions
      if (supportsUnixPermissions()) {
        Files.setPosixFilePermissions(targetProxyBinaryPath, PosixFilePermissions.fromString(PERMS_700));
      } else {
        if (!targetProxyBinaryPath.toFile().setExecutable(true)) {
          throw new IOException("Failed to make proxy binary executable");
        }
      }
      return targetProxyBinaryPath;
    } catch (Exception e) {
      LOGGER.error("Error while deploying packaged proxy binary: " + e.getMessage());
      throw e;
    }
  }

  private boolean supportsUnixPermissions() {
    return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
  }

}