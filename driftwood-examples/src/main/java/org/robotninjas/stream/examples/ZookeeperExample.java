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
package org.robotninjas.stream.examples;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.Watcher.WatcherType.Any;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

@SuppressWarnings("UnusedVariable")
public class ZookeeperExample {
   public static void main(String[] args) throws Exception {
      try (var zk = new ZKWrapper("localhost:2181")) {
         zk.create("/node/1", new byte[] {}, OPEN_ACL_UNSAFE, EPHEMERAL).flatMapMany(create ->
            zk.getChildrenAndWatch("/node/")
         );
      }
   }

   interface LeaderElection {
      String leader();
   }

   static class LeaderElectionService extends AbstractIdleService implements LeaderElection {
      private static final byte[] DefaultData = new byte[]{};
      private static final ACL DefaultAcl = new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE);
      private static final List<ACL> DefaultPerms = List.of(DefaultAcl);
      private static final String BasePath = "/election";
      private static final String CandidateName = BasePath + "/candidate";

      private final ZKWrapper client;
      private final AtomicReference<String> leader =
         new AtomicReference<>();

      private Disposable subscription = Disposables.disposed();

      LeaderElectionService(ZKWrapper client) {
         this.client = client;
      }

      private String getLeader(List<String> candidates) {
         candidates.sort(String::compareTo);
         return candidates.stream().findFirst().get();
      }

      @Override
      protected void startUp() throws Exception {
         this.subscription = client.create(CandidateName, DefaultData, DefaultPerms, EPHEMERAL_SEQUENTIAL)
            .flatMapMany(ignore -> client.watchChildren(BasePath))
            .map(children -> getLeader(children.children()))
            .subscribe(leader::set);
      }

      public String leader() {
         return leader.get();
      }

      @Override
      protected void shutDown() {
         subscription.dispose();
      }
   }

   record ZKWrapper(Mono<ZooKeeper>client) implements AutoCloseable {

      static final int DefaultSessionTimeout = 5000;

      ZKWrapper(String connectString, int sessionTimeout) {
         this(Mono.fromCallable(() -> new ZooKeeper(connectString, sessionTimeout, ignore -> {})));
      }

      ZKWrapper(String connectString) {
         this(connectString, DefaultSessionTimeout);
      }

      Mono<Create> create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
         return client.flatMap(zk ->
            Mono.create(sink ->
               zk.create(path, data, acl, createMode, (rc, path1, ctx, name, stat) -> {
                  var code = Code.get(rc);
                  if (code == Code.OK) {
                     sink.success(new Create(path1, name, stat));
                  } else {
                     sink.error(KeeperException.create(code));
                  }
               }, null)
            )
         );
      }

      Mono<Delete> delete(String path, int version) {
         return client.flatMap(zk ->
            Mono.create(sink ->
               zk.delete(path, version, (rc, path1, ctx) -> {
                  var code = Code.get(rc);
                  if (code == Code.OK) {
                     sink.success(new Delete(path1));
                  } else {
                     sink.error(KeeperException.create(code));
                  }
               }, null)
            )
         );
      }

      Flux<Watch> watch(String path, AddWatchMode mode) {
         return client.flatMapMany(zk ->
            Flux.create(sink -> {
               final Watcher watcher = e -> new Watch(e.getPath(), e.getType());

               zk.addWatch(path, watcher, mode, (rc, path1, ctx) -> {
                  var code = Code.get(rc);
                  if (code != Code.OK) {
                     sink.error(KeeperException.create(code));
                  }
               }, null);

               sink.onDispose(() -> {
                  try {
                     zk.removeWatches(path, watcher, Any, false);
                  } catch (Exception ignored) {
                  }
               });
            })
         );
      }

      Mono<Exists> exists(String path, boolean shouldWatch) {
         return client.flatMap(zk ->
            Mono.create(sink ->
               zk.exists(path, shouldWatch, (rc, path1, ctx, stat) -> {
                  var code = Code.get(rc);
                  if (code == Code.OK) {
                     sink.success(new Exists(path1, stat));
                  } else {
                     sink.error(KeeperException.create(code));
                  }
               }, null)
            )
         );
      }

      Mono<Children> getChildren(String path) {
         return client.flatMap(zk ->
            Mono.create(sink ->
               zk.getChildren(path, true, (rc, path1, ctx, children, stat) -> {
                  var code = Code.get(rc);
                  if (code == Code.OK) {
                     sink.success(new Children(path1, children, stat));
                  } else {
                     sink.error(KeeperException.create(code));
                  }
               }, null)
            )
         );
      }

      Flux<Children> watchChildren(String path) {
         return client.flatMapMany(zk ->
            Flux.create(sink ->
               zk.getChildren(path, true, (rc, path1, ctx, children, stat) -> {
                  var code = Code.get(rc);
                  if (code == Code.OK) {
                     sink.next(new Children(path1, children, stat));
                  } else {
                     sink.error(KeeperException.create(code));
                  }
               }, null)
            )
         );
      }

      Mono<Data> getData(String path) {
         return client.flatMap(zk ->
            Mono.create(sink ->
               zk.getData(path, false, (rc, path1, ctx, data, stat) -> {
                  var code = Code.get(rc);
                  if (code == Code.OK) {
                     sink.success(new Data(path1, data, stat));
                  } else {
                     sink.error(KeeperException.create(code));
                  }
               }, null)
            )
         );
      }

      Flux<Data> watchData(String path) {
         return client.flatMapMany(zk ->
            Flux.create(sink ->
               zk.getData(path, true, (rc, path1, ctx, data, stat) -> {
                  var code = Code.get(rc);
                  if (code == Code.OK) {
                     sink.next(new Data(path1, data, stat));
                  } else {
                     sink.error(KeeperException.create(code));
                  }
               }, null)
            )
         );
      }

      Flux<Children> getChildrenAndWatch(String path) {
         return getChildren(path).flatMapMany(children ->
            watchChildren(path).flatMap(watch ->
               getChildrenAndWatch(path)
            )
         );
      }

      @Override
      public void close() throws RuntimeException {
         client.flatMap(zk -> Mono.fromCallable(() -> {
            zk.close();
            return null;
         })).then().block();
      }

      record Create(String path, String name, Stat stat) {}

      record Delete(String path) {}

      record Watch(String path, EventType type) {}

      record Exists(String path, Stat stat) {}

      record Children(String path, List<String>children, Stat stat) {}

      record Data(String path, byte[]data, Stat stat) {}
   }

}
