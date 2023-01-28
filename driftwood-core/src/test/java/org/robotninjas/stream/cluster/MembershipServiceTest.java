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

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.robotninjas.stream.Config;
import org.robotninjas.stream.config.ConfigWrapper;

public class MembershipServiceTest {

  private static final Map<String, String> DefaultProperties = Map.of(
    "metadata.num.vnodes", "100"
  );

  private static MembershipConfig makeConfig(Map<String, String> properties) {
    Map<String, String> merged = new HashMap<>(DefaultProperties);
    merged.putAll(properties);
    return ConfigWrapper.proxy(MembershipConfig.class, Config.fromMap(merged));
  }

  @Test
  @Ignore
  public void testMembershipService() {
    var service1 = new ClusterService(makeConfig(Map.of(
      "membership.port", "8080"
    )));

    var service2 = new ClusterService(makeConfig(Map.of(
      "membership.port", "8081",
      "membership.seed.members", "localhost:8080"
    )));

    service1.startAsync().awaitRunning();
    service2.startAsync().awaitRunning();
  }

}