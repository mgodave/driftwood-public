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
package org.robotninjas.stream.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.google.common.util.concurrent.AbstractService;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class ZookeeperService extends AbstractService {
  private final ZooKeeperServer zks;
  private final NIOServerCnxnFactory serverFactory;
  private final InetSocketAddress address;

  public ZookeeperService(File snapDir, File logDir, int zkPort, int maxCC) throws Exception {
    zks = new ZooKeeperServer(snapDir, logDir, ZooKeeperServer.DEFAULT_TICK_TIME);
    serverFactory = new NIOServerCnxnFactory();
    address = new InetSocketAddress(zkPort);
    serverFactory.configure(address, maxCC);
  }

  public SocketAddress address() {
    return address;
  }

  @Override
  protected void doStart() {
    try {
      serverFactory.startup(zks);
    } catch (InterruptedException e) {
      notifyStopped();
    } catch (IOException ioe) {
      notifyFailed(ioe);
    }
  }

  @Override
  protected void doStop() {
    if (null != serverFactory) {
      serverFactory.shutdown();
    }
    if (null != zks) {
      zks.shutdown();
    }
    notifyStopped();
  }
}
