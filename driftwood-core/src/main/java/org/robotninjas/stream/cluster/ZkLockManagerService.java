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
package org.robotninjas.stream.cluster;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import reactor.core.publisher.Mono;

public class ZkLockManagerService extends AbstractIdleService implements LockManager {
  private final CuratorFramework curator;
  private final LockManagerConfig config;
  private final String lockPath;

  public ZkLockManagerService(String zkString, String lockPath, LockManagerConfig config) {
    this.config = config;

    this.curator = factory(config).build();
    this.lockPath = lockPath;
  }

  @Override
  protected void startUp() throws Exception {
    curator.start();
  }

  @Override
  protected void shutDown() throws Exception {
    curator.close();
  }

  @Override
  public Lock get(Resource resource) {
    var path = lockPath + "/" + resource;
    var mutex = new InterProcessMutex(curator, path);
    return new CuratorLock(mutex);
  }

  record CuratorLock(InterProcessMutex mutex) implements Lock {
    @Override
    public Mono<Void> acquire() {
      return Mono.fromCallable(() -> {
        mutex.acquire();
        return null;
      });
    }

    @Override
    public Mono<Void> release() {
      return Mono.fromCallable(() -> {
        mutex.release();
        return null;
      });
    }
  }

  private CuratorFrameworkFactory.Builder factory(LockManagerConfig config) {
    var builder = CuratorFrameworkFactory.builder();

    config.connectString().ifPresent(builder::connectString);
    builder.ensembleTracker(config.ensembleTracker());
    config.sessionTimeout().ifPresent(builder::sessionTimeoutMs);
    config.connectionTimeout().ifPresent(builder::connectionTimeoutMs);
    config.maxCloseWait().ifPresent(builder::maxCloseWaitMs);
    config.namespace().ifPresent(builder::namespace);
    builder.canBeReadOnly(config.canBeReadOnly());
    config.waitForShutdown().ifPresent(builder::waitForShutdownTimeoutMs);
    config.simulatedSessionExpirationPercent().ifPresent(builder::simulatedSessionExpirationPercent);

    return builder;
  }
}
