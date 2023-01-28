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
package org.robotninjas.stream.grpc;

import java.util.List;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.reactivestreams.Publisher;
import org.robotninjas.stream.proto.MarkEnd;
import org.robotninjas.stream.proto.ReactorReadProxyGrpc;
import org.robotninjas.stream.proto.ReactorReadProxyGrpc.ReactorReadProxyStub;
import org.robotninjas.stream.proto.ReactorReplicatorGrpc;
import org.robotninjas.stream.proto.ReactorReplicatorGrpc.ReactorReplicatorStub;
import org.robotninjas.stream.proto.ReactorWriteProxyGrpc;
import org.robotninjas.stream.proto.ReactorWriteProxyGrpc.ReactorWriteProxyStub;
import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.proto.Replicate;
import org.robotninjas.stream.proto.Truncate;
import org.robotninjas.stream.proto.Write;
import org.robotninjas.stream.security.AuthToken;
import org.robotninjas.stream.security.Permission;
import org.robotninjas.stream.test.BaseDriftwoodTest;
import org.robotninjas.stream.test.MockStreamFactory;
import org.robotninjas.stream.test.ReadOnlyStreamFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class GrpcServiceAuthTests extends BaseDriftwoodTest {
  @Nested
  @DisplayName("ReadProxy")
  class ReadProxyTests {
    public <T> Executable readProxyTest(ReadProxyCall<T> caller, String username, String password, List<Permission> perms, Expect<T> expect) {
      return () -> {
        // Server side config
        var servername = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(servername).directExecutor();
        var streamFactory = ReadOnlyStreamFactory.create(Streamname, Flux::empty);
        var security = makeSecurity(perms);
        var service = new GrpcService(server, GrpcReadProxy.server(streamFactory, security));

        service.startAsync().awaitRunning();
        try {
          // Client side config
          var channel = InProcessChannelBuilder.forName(servername).directExecutor().build();
          var token = AuthToken.fromUsernameAndPassword(username, password);
          var stub = ReactorReadProxyGrpc.newReactorStub(channel)
            .withCallCredentials(new AuthTokenCallCredentials(token));

          Publisher<T> result = caller.call(stub);
          var firstStep = StepVerifier.create(result);
          expect.step(firstStep).verify();

          service.stopAsync().awaitTerminated();
        } catch (RuntimeException e) {
          if (service.isRunning()) {
            service.stopAsync();
          }
          throw e;
        }
      };
    }

    @TestFactory
    public List<DynamicTest> readProxyTests() {
      final ReadProxyCall<Read.Response> CallRead = stub -> stub.read(makeReadRequest(Streamname));

      return List.of(
        dynamicTest("Read: Valid username, password, and permissions", readProxyTest(
          CallRead, ValidUsername, ValidPassword, ReadPermissions, Expect::success
        )),
        dynamicTest("Read: Invalid username, valid password and permissions", readProxyTest(
          CallRead, InvalidUsername, ValidPassword, ReadPermissions, Expect::unauthenticated
        )),
        dynamicTest("Read: Invalid password, valid username and permissions", readProxyTest(
          CallRead, ValidUsername, InvalidPassword, ReadPermissions, Expect::unauthenticated
        )),
        dynamicTest("Read: Invalid permissions, valid username and password", readProxyTest(
          CallRead, ValidUsername, ValidPassword, NoPermissions, Expect::permissionDenied
        )),
        dynamicTest("Read: Valid username, password, all permissions", readProxyTest(
          CallRead, ValidUsername, ValidPassword, AllPermissions, Expect::success
        ))
      );
    }
  }

  @Nested
  @DisplayName("WriteProxy")
  class WriteProxyTests {
    public <T> Executable writeProxyTest(WriteProxyCall<T> caller, String username, String password, List<Permission> perms, Expect<T> expect) {
      return () -> {
        // Server side config
        var servername = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(servername).directExecutor();
        var streamFactory = new MockStreamFactory(Flux.empty());
        var security = makeSecurity(perms);
        var service = new GrpcService(server, GrpcWriteProxy.server(streamFactory, security));

        service.startAsync().awaitRunning();
        try {
          // Client side config
          var channel = InProcessChannelBuilder.forName(servername).directExecutor().build();
          var token = AuthToken.fromUsernameAndPassword(username, password);
          var stub = ReactorWriteProxyGrpc.newReactorStub(channel).withCallCredentials(new AuthTokenCallCredentials(token));

          // The call
          Publisher<T> result = caller.call(stub);
          var firstStep = StepVerifier.create(result);
          expect.step(firstStep).verify();

          service.stopAsync().awaitTerminated();
        } catch (RuntimeException e) {
          if (service.isRunning()) {
            service.stopAsync();
          }
          throw e;
        }
      };
    }

    @TestFactory
    public List<DynamicTest> writeProxyTests() {
      final WriteProxyCall<Write.Response> CallWrite = stub -> stub.write(makeWriteRequest(Streamname));
      final WriteProxyCall<Truncate.Response> CallTruncate = stub -> stub.truncate(makeTruncateRequest(Streamname));
      final WriteProxyCall<MarkEnd.Response> CallMarkEnd = stub -> stub.markEnd(makeMarkEndRequest(Streamname));

      return List.of(
        dynamicTest("Write: Valid username, password, and permissions", writeProxyTest(
          CallWrite, ValidUsername, ValidPassword, WritePermissions, Expect::success
        )),
        dynamicTest("Write: Invalid username, valid password and permissions", writeProxyTest(
          CallWrite, InvalidUsername, ValidPassword, WritePermissions, Expect::unauthenticated
        )),
        dynamicTest("Write: Invalid password, valid username and permissions", writeProxyTest(
          CallWrite, ValidUsername, InvalidPassword, WritePermissions, Expect::unauthenticated
        )),
        dynamicTest("Write: Invalid permissions, valid username and password", writeProxyTest(
          CallWrite, ValidUsername, ValidPassword, NoPermissions, Expect::permissionDenied
        )),
        dynamicTest("Write: Valid username, password, all permissions", writeProxyTest(
          CallWrite, ValidUsername, ValidPassword, AllPermissions, Expect::success
        )),
        dynamicTest("Truncate: Valid username, password, and permissions", writeProxyTest(
          CallTruncate, ValidUsername, ValidPassword, TruncatePermissions, Expect::response
        )),
        dynamicTest("Truncate: Invalid username, valid password and permissions", writeProxyTest(
          CallTruncate, InvalidUsername, ValidPassword, TruncatePermissions, Expect::unauthenticated
        )),
        dynamicTest("Truncate: Invalid password, valid username and permissions", writeProxyTest(
          CallTruncate, ValidUsername, InvalidPassword, TruncatePermissions, Expect::unauthenticated
        )),
        dynamicTest("Truncate: Invalid permissions, valid username and password", writeProxyTest(
          CallTruncate, ValidUsername, ValidPassword, NoPermissions, Expect::permissionDenied
        )),
        dynamicTest("Truncate: Valid username, password, all permissions", writeProxyTest(
          CallTruncate, ValidUsername, ValidPassword, AllPermissions, Expect::response
        )),
        dynamicTest("MarkEnd: Valid username, password, and permissions", writeProxyTest(
          CallMarkEnd, ValidUsername, ValidPassword, MarkEndPermissions, Expect::response
        )),
        dynamicTest("MarkEnd: Invalid username, valid password and permissions", writeProxyTest(
          CallMarkEnd, InvalidUsername, ValidPassword, MarkEndPermissions, Expect::unauthenticated
        )),
        dynamicTest("MarkEnd: Invalid password, valid username and permissions", writeProxyTest(
          CallMarkEnd, ValidUsername, InvalidPassword, MarkEndPermissions, Expect::unauthenticated
        )),
        dynamicTest("MarkEnd: Invalid permissions, valid username and password", writeProxyTest(
          CallMarkEnd, ValidUsername, ValidPassword, NoPermissions, Expect::permissionDenied
        )),
        dynamicTest("MarkEnd: Valid username, password, all permissions", writeProxyTest(
          CallMarkEnd, ValidUsername, ValidPassword, AllPermissions, Expect::response
        ))
      );
    }
  }

  @Nested
  @DisplayName("Replicator")
  class ReplicatorTests {
    public <T> Executable replicatorTest(ReplicatorCall<T> caller, String username, String password, List<Permission> perms, Expect<T> expect) {
      return () -> {
        // Server side config
        var servername = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(servername).directExecutor();
        var streamFactory = new MockStreamFactory(Flux.empty());
        var security = makeSecurity(perms);
        var service = new GrpcService(server, GrpcReplicator.server(streamFactory, security));

        service.startAsync().awaitRunning();
        try {
          // Client side config
          var channel = InProcessChannelBuilder.forName(servername).directExecutor().build();
          var token = AuthToken.fromUsernameAndPassword(username, password);
          var stub = ReactorReplicatorGrpc.newReactorStub(channel).withCallCredentials(new AuthTokenCallCredentials(token));

          // The call
          Publisher<T> result = caller.call(stub);
          var firstStep = StepVerifier.create(result);
          expect.step(firstStep).verify();

          service.stopAsync().awaitTerminated();
        } catch (RuntimeException e) {
          if (service.isRunning()) {
            service.stopAsync();
          }
          throw e;
        }
      };
    }

    @TestFactory
    public List<DynamicTest> replicatorTests() {
      final ReplicatorCall<Replicate.Response> CallWrite = stub -> stub.write(makeReplicateWriteRequest(Streamname));
      final ReplicatorCall<Replicate.Transfer> CallRead = stub -> stub.read(makeReplicateReadRequest(Streamname));

      return List.of(
        dynamicTest("Write: Valid username, password, and permissions", replicatorTest(
          CallWrite, ValidUsername, ValidPassword, PushPermissions, Expect::response
        )),
        dynamicTest("Write: Invalid username, valid password and permissions", replicatorTest(
          CallWrite, InvalidUsername, ValidPassword, PushPermissions, Expect::unauthenticated
        )),
        dynamicTest("Write: Invalid password, valid username and permissions", replicatorTest(
          CallWrite, ValidUsername, InvalidPassword, PushPermissions, Expect::unauthenticated
        )),
        dynamicTest("Write: Invalid permissions, valid username and password", replicatorTest(
          CallWrite, ValidUsername, ValidPassword, NoPermissions, Expect::permissionDenied
        )),
        dynamicTest("Write: Valid username, password, all permissions", replicatorTest(
          CallWrite, ValidUsername, ValidPassword, AllPermissions, Expect::response
        )),
        dynamicTest("Read: Valid username, password, and permissions", replicatorTest(
          CallRead, ValidUsername, ValidPassword, PullPermissions, Expect::success
        )),
        dynamicTest("Read: Invalid username, valid password and permissions", replicatorTest(
          CallRead, InvalidUsername, ValidPassword, PullPermissions, Expect::unauthenticated
        )),
        dynamicTest("Read: Invalid password, valid username and permissions", replicatorTest(
          CallRead, ValidUsername, InvalidPassword, PullPermissions, Expect::unauthenticated
        )),
        dynamicTest("Read: Invalid permissions, valid username and password", replicatorTest(
          CallRead, ValidUsername, ValidPassword, NoPermissions, Expect::permissionDenied
        )),
        dynamicTest("Read: Valid username, password, all permissions", replicatorTest(
          CallRead, ValidUsername, ValidPassword, AllPermissions, Expect::success
        ))
      );
    }
  }

  @FunctionalInterface
  private interface ReadProxyCall<T> {
    Publisher<T> call(ReactorReadProxyStub stub);
  }

  @FunctionalInterface
  private interface WriteProxyCall<T> {
    Publisher<T> call(ReactorWriteProxyStub stub);
  }

  @FunctionalInterface
  private interface ReplicatorCall<T> {
    Publisher<T> call(ReactorReplicatorStub stub);
  }

  @FunctionalInterface
  interface Expect<T> {
    StepVerifier step(StepVerifier.FirstStep<T> fn);

    static <T> StepVerifier unauthenticated(StepVerifier.FirstStep<T> verifier) {
      return verifier.expectErrorMatches(e ->
        e instanceof StatusRuntimeException sre && sre.getStatus().equals(Status.UNAUTHENTICATED)
      );
    }

    static <T> StepVerifier permissionDenied(StepVerifier.FirstStep<T> verifier) {
      return verifier.expectErrorMatches(e ->
        e instanceof StatusRuntimeException sre && sre.getStatus().equals(Status.PERMISSION_DENIED)
      );
    }

    static <T> StepVerifier response(StepVerifier.FirstStep<T> verifier) {
      return verifier.expectNextCount(1).expectComplete();
    }

    static <T> StepVerifier success(StepVerifier.FirstStep<T> verifier) {
      return verifier.expectComplete();
    }
  }
}