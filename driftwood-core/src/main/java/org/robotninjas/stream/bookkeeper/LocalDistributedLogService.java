/**
 * Copyright [2023] David J. Rusek <dave.rusek@gmail.com>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.robotninjas.stream.bookkeeper;

import java.net.URI;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.distributedlog.LocalDLMEmulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalDistributedLogService extends AbstractIdleService implements LocalDistributedLog {
  private static final Logger Log = LoggerFactory.getLogger(LocalDistributedLogService.class);

  private final LocalDLMEmulator dlEmulator;

  LocalDistributedLogService(LocalDLMEmulator dlmEmulator) {
    this.dlEmulator = dlmEmulator;
  }

  @Override
  protected void startUp() throws Exception {
    dlEmulator.start();
  }

  @Override
  protected void shutDown() throws Exception {
    dlEmulator.teardown();
  }

  @Override
  public URI uri() {
    return dlEmulator.getUri();
  }

  public static LocalDistributedLogService withExternalZk(ServerConfiguration bkConf, ZkConfig zkConfig) throws Exception {
    return new LocalDistributedLogService(
      LocalDLMEmulator.newBuilder()
        .numBookies(1)
        .shouldStartZK(false)
        .zkHost(zkConfig.host)
        .zkPort(zkConfig.port)
        .zkTimeoutSec(zkConfig.timeout)
        .serverConf(bkConf)
        .build()
    );
  }

  public static LocalDistributedLogService standalone(ServerConfiguration bkConf, int numBookies) throws Exception {
    return new LocalDistributedLogService(
      LocalDLMEmulator.newBuilder()
        .numBookies(numBookies)
        .shouldStartZK(true)
        .serverConf(bkConf)
        .build()
    );
  }

  public static LocalDistributedLogService standalone(ServerConfiguration bkConf) throws Exception {
    return standalone(bkConf, 1);
  }

  record ZkConfig(String host, int port, int timeout) {
    public ZkConfig(String host, int port) {
      this(host, port, 10);
    }

    public ZkConfig(String host) {
      this(host, 2181);
    }
  }
}
