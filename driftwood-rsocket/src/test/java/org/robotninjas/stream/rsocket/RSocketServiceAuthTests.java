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
package org.robotninjas.stream.rsocket;

import java.util.List;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.rsocket.core.RSocketConnector;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.transport.local.LocalServerTransport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.reactivestreams.Publisher;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.proto.*;
import org.robotninjas.stream.security.AuthToken;
import org.robotninjas.stream.security.Permission;
import org.robotninjas.stream.test.BaseDriftwoodTest;
import org.robotninjas.stream.test.MockStreamFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class RSocketServiceAuthTests extends BaseDriftwoodTest {
  private static final List<Replicate.Record> ReplicateRecords = List.of(
    Replicate.Record.newBuilder().setData(ByteString.EMPTY).setOffset(ByteString.EMPTY).setTxid(-1).build()
  );

  private static final List<Write.Record> WriteRecords = List.of(
    Write.Record.newBuilder().setData(ByteString.EMPTY).setTxid(-1).build()
  );

  private static final Start StartNewest = Start.newBuilder().setFromNewest(Start.FromNewest.newBuilder().build()).build();

  @Nested
  @DisplayName("ReadProxy")
  class ReadProxyTests {
    public <T> Executable readProxyTest(ReadProxyCall<T> caller, String username, String password, List<Permission> perms, Expect<T> expect) {
      return () -> {
        var server = LocalServerTransport.createEphemeral();
        var streamFactory = new MockStreamFactory(Flux.empty());

        var service = new RSocketService(server, RSocketReadProxy.server(streamFactory, makeSecurity(perms)));
        service.startAsync().awaitRunning();

        var client = RSocketConnector.create()
          .connect(server.clientTransport())
          .map(ReadProxyClient::new);

        var token = AuthToken.fromUsernameAndPassword(username, password);
        var response = client.flatMapMany(c -> caller.call(c, Metadata.make(token)));

        service.stopAsync().awaitTerminated();

        expect.step(StepVerifier.create(response)).verify();
      };
    }

    @TestFactory
    public List<DynamicTest> readProxyTests() {
      return List.of(
        dynamicTest("Read: Valid username, password, and permissions", readProxyTest(
          ReadProxyCall.read(Streamname, StartNewest), ValidUsername, ValidPassword, ReadPermissions, Expect::success
        )),
        dynamicTest("Read: Invalid username, valid password and permissions", readProxyTest(
          ReadProxyCall.read(Streamname, StartNewest), InvalidUsername, ValidPassword, ReadPermissions, Expect::applicationError
        )),
        dynamicTest("Read: Invalid password, valid username and permissions", readProxyTest(
          ReadProxyCall.read(Streamname, StartNewest), ValidUsername, InvalidPassword, ReadPermissions, Expect::applicationError
        )),
        dynamicTest("Read: Invalid permissions, valid username and password", readProxyTest(
          ReadProxyCall.read(Streamname, StartNewest), ValidUsername, ValidPassword, NoPermissions, Expect::applicationError
        )),
        dynamicTest("Read: Valid username, password, all permissions", readProxyTest(
          ReadProxyCall.read(Streamname, StartNewest), ValidUsername, ValidPassword, AllPermissions, Expect::success
        ))
      );
    }
  }

  @Nested
  @DisplayName("ReadProxy")
  class WriteProxyTests {
    public <T> Executable writeProxyTest(WriteProxyCall<T> caller, String username, String password, List<Permission> perms, Expect<T> expect) {
      return () -> {
        var server = LocalServerTransport.createEphemeral();
        var streamFactory = new MockStreamFactory(Flux.empty());

        var service = new RSocketService(server, RSocketWriteProxy.server(streamFactory, makeSecurity(perms)));
        service.startAsync().awaitRunning();

        var client = RSocketConnector.create()
          .connect(server.clientTransport())
          .map(WriteProxyClient::new);

        var token = AuthToken.fromUsernameAndPassword(username, password);
        var response = client.flatMapMany(c -> caller.call(c, Metadata.make(token)));

        service.stopAsync().awaitTerminated();

        expect.step(StepVerifier.create(response)).verify();
      };
    }

    @TestFactory
    public List<DynamicTest> writeProxyTests() {
      return List.of(
        dynamicTest("Write: Valid username, password, and permissions", writeProxyTest(
          WriteProxyCall.write(Streamname, WriteRecords), ValidUsername, ValidPassword, WritePermissions, Expect::response
        )),
        dynamicTest("Write: Invalid username, valid password and permissions", writeProxyTest(
          WriteProxyCall.write(Streamname, WriteRecords), InvalidUsername, ValidPassword, WritePermissions, Expect::applicationError
        )),
        dynamicTest("Write: Invalid password, valid username and permissions", writeProxyTest(
          WriteProxyCall.write(Streamname, WriteRecords), ValidUsername, InvalidPassword, WritePermissions, Expect::applicationError
        )),
        dynamicTest("Write: Invalid permissions, valid username and password", writeProxyTest(
          WriteProxyCall.write(Streamname, WriteRecords), ValidUsername, ValidPassword, NoPermissions, Expect::applicationError
        )),
        dynamicTest("Write: Valid username, password, all permissions", writeProxyTest(
          WriteProxyCall.write(Streamname, WriteRecords), ValidUsername, ValidPassword, AllPermissions, Expect::response
        )),
        dynamicTest("Truncate: Valid username, password, and permissions", writeProxyTest(
          WriteProxyCall.truncate(Streamname), ValidUsername, ValidPassword, TruncatePermissions, Expect::response
        )),
        dynamicTest("Truncate: Invalid username, valid password and permissions", writeProxyTest(
          WriteProxyCall.truncate(Streamname), InvalidUsername, ValidPassword, TruncatePermissions, Expect::applicationError
        )),
        dynamicTest("Truncate: Invalid password, valid username and permissions", writeProxyTest(
          WriteProxyCall.truncate(Streamname), ValidUsername, InvalidPassword, TruncatePermissions, Expect::applicationError
        )),
        dynamicTest("Truncate: Invalid permissions, valid username and password", writeProxyTest(
          WriteProxyCall.truncate(Streamname), ValidUsername, ValidPassword, NoPermissions, Expect::applicationError
        )),
        dynamicTest("Truncate: Valid username, password, all permissions", writeProxyTest(
          WriteProxyCall.truncate(Streamname), ValidUsername, ValidPassword, AllPermissions, Expect::response
        )),
        dynamicTest("MarkEnd: Valid username, password, and permissions", writeProxyTest(
          WriteProxyCall.markEnd(Streamname), ValidUsername, ValidPassword, MarkEndPermissions, Expect::response
        )),
        dynamicTest("MarkEnd: Invalid username, valid password and permissions", writeProxyTest(
          WriteProxyCall.markEnd(Streamname), InvalidUsername, ValidPassword, MarkEndPermissions, Expect::applicationError
        )),
        dynamicTest("MarkEnd: Invalid password, valid username and permissions", writeProxyTest(
          WriteProxyCall.markEnd(Streamname), ValidUsername, InvalidPassword, MarkEndPermissions, Expect::applicationError
        )),
        dynamicTest("MarkEnd: Invalid permissions, valid username and password", writeProxyTest(
          WriteProxyCall.markEnd(Streamname), ValidUsername, ValidPassword, NoPermissions, Expect::applicationError
        )),
        dynamicTest("MarkEnd: Valid username, password, all permissions", writeProxyTest(
          WriteProxyCall.markEnd(Streamname), ValidUsername, ValidPassword, AllPermissions, Expect::response
        ))
      );
    }
  }

  @Nested
  @DisplayName("Replicator")
  class ReplicatorTests {
    public <T> Executable replicatorTest(ReplicatorCall<T> caller, String username, String password, List<Permission> perms, Expect<T> expect) {
      return () -> {
        var server = LocalServerTransport.createEphemeral();
        var streamFactory = new MockStreamFactory(Flux.empty());

        var service = new RSocketService(server, RSocketReplicator.server(streamFactory, makeSecurity(perms)));
        service.startAsync().awaitRunning();

        var client = RSocketConnector.create()
          .connect(server.clientTransport())
          .map(ReplicatorClient::new);

        var token = AuthToken.fromUsernameAndPassword(username, password);
        var response = client.flatMapMany(c -> caller.call(c, Metadata.make(token)));

        service.stopAsync().awaitTerminated();

        expect.step(StepVerifier.create(response)).verify();
      };
    }

    @TestFactory
    public List<DynamicTest> replicatorTests() {
      return List.of(
        dynamicTest("Write: Valid username, password, and permissions", replicatorTest(
          ReplicatorCall.write(Streamname, ReplicateRecords), ValidUsername, ValidPassword, PushPermissions, Expect::response
        )),
        dynamicTest("Write: Invalid username, valid password and permissions", replicatorTest(
          ReplicatorCall.write(Streamname, ReplicateRecords), InvalidUsername, ValidPassword, PushPermissions, Expect::applicationError
        )),
        dynamicTest("Write: Invalid password, valid username and permissions", replicatorTest(
          ReplicatorCall.write(Streamname, ReplicateRecords), ValidUsername, InvalidPassword, PushPermissions, Expect::applicationError
        )),
        dynamicTest("Write: Invalid permissions, valid username and password", replicatorTest(
          ReplicatorCall.write(Streamname, ReplicateRecords), ValidUsername, ValidPassword, NoPermissions, Expect::applicationError
        )),
        dynamicTest("Write: Valid username, password, all permissions", replicatorTest(
          ReplicatorCall.write(Streamname, ReplicateRecords), ValidUsername, ValidPassword, AllPermissions, Expect::response
        )),
        dynamicTest("Read: Valid username, password, and permissions", replicatorTest(
          ReplicatorCall.read(Streamname, StartNewest), ValidUsername, ValidPassword, PullPermissions, Expect::success
        )),
        dynamicTest("Read: Invalid username, valid password and permissions", replicatorTest(
          ReplicatorCall.read(Streamname, StartNewest), InvalidUsername, ValidPassword, PullPermissions, Expect::applicationError
        )),
        dynamicTest("Read: Invalid password, valid username and permissions", replicatorTest(
          ReplicatorCall.read(Streamname, StartNewest), ValidUsername, InvalidPassword, PullPermissions, Expect::applicationError
        )),
        dynamicTest("Read: Invalid permissions, valid username and password", replicatorTest(
          ReplicatorCall.read(Streamname, StartNewest), ValidUsername, ValidPassword, NoPermissions, Expect::applicationError
        )),
        dynamicTest("Read: Valid username, password, all permissions", replicatorTest(
          ReplicatorCall.read(Streamname, StartNewest), ValidUsername, ValidPassword, AllPermissions, Expect::success
        ))
      );
    }
  }

  @FunctionalInterface
  private interface ReadProxyCall<T> {
    Publisher<T> call(ReadProxyClient stub, ByteBuf metadata);

    private static Read.Request readRequest(StreamName stream, Start start) {
      return Read.Request.newBuilder()
        .setName(stream.name())
        .setStart(start)
        .build();
    }

    static ReadProxyCall<Read.Response> read(StreamName streamName, Start start) {
      return (stub, metadata) -> stub.read(readRequest(streamName, start), metadata);
    }

  }

  @FunctionalInterface
  private interface WriteProxyCall<T> {
    Publisher<T> call(WriteProxyClient stub, ByteBuf metadata);

    private static Flux<Write.Request> writeRequest(StreamName stream, List<Write.Record> records) {
      return Flux.just(
        Write.Request.newBuilder()
          .addAllData(records)
          .setStream(stream.name())
          .build()
      );
    }

    private static Truncate.Request truncateRequest(StreamName stream) {
      return Truncate.Request.newBuilder()
        .setStream(stream.name())
        .setOffset(ByteString.copyFrom(Offset.Empty.bytes()))
        .build();
    }

    private static MarkEnd.Request markEndRequest(StreamName stream) {
      return MarkEnd.Request.newBuilder()
        .setStream(stream.name())
        .build();
    }

    static WriteProxyCall<Write.Response> write(StreamName streamName, List<Write.Record> records){
      return (stub, metadata) -> stub.write(writeRequest(streamName, records), metadata);
    }

    static WriteProxyCall<Write.Response> write(StreamName streamName, Write.Record... records){
      return write(streamName, List.of(records));
    }

    static WriteProxyCall<Truncate.Response> truncate(StreamName streamName)  {
      return (stub, metadata) -> stub.truncate(truncateRequest(streamName), metadata);
    }

    static WriteProxyCall<MarkEnd.Response> markEnd(StreamName streamName) {
      return (stub, metadata) -> stub.markEnd(markEndRequest(streamName), metadata);
    }
  }

  @FunctionalInterface
  private interface ReplicatorCall<T> {
    Publisher<T> call(ReplicatorClient stub, ByteBuf metadata);

    private static Replicate.Request replicateReadRequest(StreamName stream, Start start) {
      return Replicate.Request.newBuilder()
        .setName(stream.name())
        .setStart(start)
        .build();
    }

    private static Flux<Replicate.Transfer> replicateWriteRequest(StreamName stream, List<Replicate.Record> records) {
      return Flux.just(
        Replicate.Transfer.newBuilder()
          .setStream(stream.name())
          .addAllData(records)
          .build()
      );
    }

    static ReplicatorCall<Replicate.Response> write(StreamName streamName, List<Replicate.Record> records) {
      return (stub, metadata) -> stub.write(replicateWriteRequest(streamName, records), metadata);
    }

    static ReplicatorCall<Replicate.Response> write(StreamName streamName, Replicate.Record... records) {
      return write(streamName, List.of(records));
    }

    static ReplicatorCall<Replicate.Transfer> read(StreamName streamName, Start start) {
      return (stub, metadata) -> stub.read(replicateReadRequest(streamName, start), metadata);
    }

  }

  @FunctionalInterface
  interface Expect<T> {
    StepVerifier step(StepVerifier.Step<T> fn);

    static <T> StepVerifier applicationError(StepVerifier.Step<T> verifier) {
      return verifier.expectError(ApplicationErrorException.class);
    }

    static <T> StepVerifier response(StepVerifier.Step<T> verifier) {
      return verifier.expectNextCount(1).expectComplete();
    }

    static <T> StepVerifier success(StepVerifier.Step<T> verifier) {
      return verifier.expectComplete();
    }
  }
}
