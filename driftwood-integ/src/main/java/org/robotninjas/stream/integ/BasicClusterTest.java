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
package org.robotninjas.stream.integ;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.function.Consumer;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LocalDLMEmulator;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.SimpleAccountRealm;
import org.awaitility.Awaitility;
import org.robotninjas.app.dl.DLWriter;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.security.AuthToken;
import org.robotninjas.stream.security.Security;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.robotninjas.stream.security.Permissions.all;

public interface BasicClusterTest {
  String Username = "username";
  String Password = "password";
  String Streamname = "messaging-stream-1";
  String DefaultNamespace = "/messaging/distributedlog";
  String DLProtocolString = "distributedlog://";
  AuthToken SuperUserAuth = AuthToken.fromUsernameAndPassword(Username, Password);
  int NumMessages = 100;

  DistributedLogConfiguration DlConfig =
    new DistributedLogConfiguration()
      .setCreateStreamIfNotExists(true)
      .setImmediateFlushEnabled(true);

  default Security noSecurity() {
    var realm = new SimpleAccountRealm();
    realm.addAccount(Username, Password, all(StreamName.of(Streamname)).toString());
    return new Security.ShiroSecurity(new DefaultSecurityManager(realm));
  }

  static void withNamespace(Consumer<Namespace> work) throws Exception {
    File zkDir = Files.createTempDirectory("zk").toFile();
    Pair<ZooKeeperServerShim, Integer> zk =
      LocalDLMEmulator.runZookeeperOnAnyPort(zkDir);
    ZooKeeperServerShim zookeeper = zk.getLeft();

    var localDLMEmulator = LocalDLMEmulator.newBuilder()
      .numBookies(3)
      .shouldStartZK(false)
      .zkHost("127.0.0.1")
      .zkPort(zk.getRight())
      .build();

    localDLMEmulator.start();

    String zkServers = localDLMEmulator.getZkServers();
    String dluri = DLProtocolString + zkServers + DefaultNamespace;
    NamespaceBuilder builder = NamespaceBuilder.newBuilder()
      .conf(DlConfig)
      .uri(new URI(dluri));

    try(Namespace ns = builder.build()) {
      work.accept(ns);
    } finally {
      localDLMEmulator.teardown();
      zookeeper.stop();
    }
  }

  default void writeData(Namespace dlns, String name, int writeCount) {
    new DLWriter(dlns, name, writeCount, 10).startAsync();
  }

  default void awaitProcessed(CountingProcessor processor, int num) {
    Awaitility.await()
      .atMost(Duration.ofSeconds(60))
      .untilAtomic(processor.numProcessed, is(equalTo(num)));
  }

  default void awaitConsumed(CountingConsumer consumer, int num) {
    Awaitility.await()
      .atMost(Duration.ofSeconds(60))
      .untilAtomic(consumer.numConsumed, is(equalTo(num)));
  }
}
