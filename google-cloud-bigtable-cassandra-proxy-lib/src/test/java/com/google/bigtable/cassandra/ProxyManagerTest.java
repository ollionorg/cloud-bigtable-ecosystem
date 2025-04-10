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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProxyManagerTest {

  private ProxyManager proxyManager;

  @BeforeEach
  void init() {
    proxyManager = new ProxyManager(new ProxyStub()); // ProxyStub is already setup
  }

  @Test
  public void testSetupProxy_Twice_Fail() {
    // ProxyManager with ProxyStub is already setup

    assertThrows(IllegalStateException.class, proxyManager::setupProxy);
  }

  @Test
  public void testStart_BeforeSetup_Fail() {
    ProxyManager proxyManager = new ProxyManager(
        BigtableCqlConfiguration.builder()
            .setProjectId("someProjectId")
            .setInstanceId("someInstanceId")
            .setSchemaMappingTable("someSchemaMappingTable")
            .build());

    assertThrows(IllegalStateException.class, proxyManager::start);
  }

  @Test
  public void testStart_AfterSetup_Succeed() {
    // ProxyManager with ProxyStub is already setup

    assertDoesNotThrow(proxyManager::start);
  }

  @Test
  public void testGetProxyPort_BeforeStart_Fail() {
    // ProxyManager with ProxyStub is already setup

    assertThrows(IllegalStateException.class, proxyManager::getProxyPort);
  }

  @Test
  public void testGetProxyPort_AfterStart_Succeed() throws IOException {
    // ProxyManager with ProxyStub is already setup
    proxyManager.start();

    assertEquals(1, proxyManager.getProxyPort());
  }

  @Test
  public void testStop_BeforeStart_Fail() {
    // ProxyManager with ProxyStub is already setup

    // Need to start proxy before stopping it
    assertThrows(IllegalStateException.class, proxyManager::stop);
  }


  @Test
  public void testStop_AfterStart_Succeed() throws IOException {
    // ProxyManager with ProxyStub is already setup
    proxyManager.start();

    assertDoesNotThrow(proxyManager::stop);
  }

  @Test
  public void testMisspecifiedProxyManagerConstruction_Fail() {
    // Null SchemaConfig
    BigtableCqlConfiguration nullBigtableCqlConfiguration = null;
    assertThrows(NullPointerException.class,
        () -> new ProxyManager(nullBigtableCqlConfiguration));

    // Null Proxy
    Proxy nullProxy = null;
    assertThrows(NullPointerException.class, () -> new ProxyManager(nullProxy));
  }

}