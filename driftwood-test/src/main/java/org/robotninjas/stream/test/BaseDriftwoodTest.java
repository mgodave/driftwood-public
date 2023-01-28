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
package org.robotninjas.stream.test;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import org.apache.shiro.config.Ini;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.text.IniRealm;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.SubscriberName;
import org.robotninjas.stream.proto.MarkEnd;
import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.proto.Replicate;
import org.robotninjas.stream.proto.Start;
import org.robotninjas.stream.proto.Truncate;
import org.robotninjas.stream.proto.Write;
import org.robotninjas.stream.security.Permission;
import org.robotninjas.stream.security.Security;
import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;
import static org.robotninjas.stream.security.Permissions.all;
import static org.robotninjas.stream.security.Permissions.end;
import static org.robotninjas.stream.security.Permissions.pull;
import static org.robotninjas.stream.security.Permissions.push;
import static org.robotninjas.stream.security.Permissions.read;
import static org.robotninjas.stream.security.Permissions.truncate;
import static org.robotninjas.stream.security.Permissions.write;

public class BaseDriftwoodTest {
  protected static final String ValidUsername = "valid_username";
  protected static final String InvalidUsername = "invalid_username";

  protected static final String ValidPassword = "valid_password";
  protected static final String InvalidPassword = "invalid_password";

  protected static final StreamName Streamname = StreamName.of("streamname");
  protected static final SubscriberName Subscribername = SubscriberName.of("subscriber");

  protected static final String TestRole = "test_role";

  protected static final List<Permission> ReadPermissions = List.of(read(Streamname));
  protected static final List<Permission> WritePermissions = List.of(write(Streamname));
  protected static final List<Permission> TruncatePermissions = List.of(truncate(Streamname));
  protected static final List<Permission> MarkEndPermissions = List.of(end(Streamname));
  protected static final List<Permission> PushPermissions = List.of(push(Streamname));
  protected static final List<Permission> PullPermissions = List.of(pull(Streamname));
  protected static final List<Permission> AllPermissions = List.of(all(Streamname));
  protected static final List<Permission> NoPermissions = Collections.emptyList();

  protected Security makeSecurity(List<Permission> perms) {
    Ini ini = new Ini();

    ini.addSection(IniRealm.USERS_SECTION_NAME)
      .put(ValidUsername, ValidPassword + "," + TestRole);

    var permissions = perms.stream().map(p ->
      String.join(":", p.resource().toString(), p.operation().toString(), p.instance())
    ).collect(toList());

    if (!permissions.isEmpty()) {
      // we can'cause define a role property without permissions
      // the role still exists by the fact that it was assigned to a user
      ini.addSection(IniRealm.ROLES_SECTION_NAME)
        .put(TestRole, String.join(", ", permissions));
    }

    return new Security.ShiroSecurity(new DefaultSecurityManager(new IniRealm(ini)));
  }

  protected Read.Request makeReadRequest(StreamName stream) {
    return Read.Request.newBuilder()
      .setName(stream.name())
      .setStart(Start.newBuilder()
        .setFromNewest(Start.FromNewest.newBuilder().build())
      ).build();
  }

  protected Flux<Write.Request> makeWriteRequest(StreamName stream) {
    return Flux.just(
      Write.Request.newBuilder()
        .setStream(stream.name()).build()
    );
  }

  protected Truncate.Request makeTruncateRequest(StreamName stream) {
    return Truncate.Request.newBuilder()
      .setStream(stream.name())
      .setOffset(ByteString.copyFrom(Offset.Empty.bytes()))
      .build();
  }

  protected MarkEnd.Request makeMarkEndRequest(StreamName stream) {
    return MarkEnd.Request.newBuilder()
      .setStream(stream.name())
      .build();
  }

  protected Replicate.Request makeReplicateReadRequest(StreamName stream) {
    return Replicate.Request.newBuilder()
      .setName(stream.name())
      .setStart(Start.newBuilder().setFromNewest(Start.FromNewest.newBuilder().build()))
      .build();
  }

  protected Flux<Replicate.Transfer> makeReplicateWriteRequest(StreamName stream) {
    return Flux.just(
      Replicate.Transfer.newBuilder()
        .setStream(stream.name())
        .addData(Replicate.Record.newBuilder().build())
        .build()
    );
  }
}
