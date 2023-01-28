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

import java.util.Arrays;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Test;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.TxId;
import org.robotninjas.stream.dlog.DLStreamFactory;
import org.robotninjas.stream.grpc.GrpcReadProxy;
import org.robotninjas.stream.grpc.GrpcService;
import org.robotninjas.stream.grpc.GrpcStreamReader;
import org.robotninjas.stream.grpc.GrpcStreamWriter;
import org.robotninjas.stream.grpc.GrpcWriteProxy;
import reactor.core.publisher.Flux;

public class GrpcEndToEndTest implements BasicClusterTest {
  @Test
  public void writeSomeStuffAndReadIt() throws Exception {
    final String baseName = GrpcEndToEndTest.class.getName();

    final ServerBuilder<? extends ServerBuilder<?>> readServer =
      InProcessServerBuilder.forName(baseName + "_read");
    final ManagedChannelBuilder<? extends ManagedChannelBuilder<?>> readClient =
      InProcessChannelBuilder.forName(baseName + "_read");

    final ServerBuilder<? extends ServerBuilder<?>> writeServer =
      InProcessServerBuilder.forName(baseName + "_write");
    final ManagedChannelBuilder<? extends ManagedChannelBuilder<?>> writeClient =
      InProcessChannelBuilder.forName(baseName + "_write");

    var bytes = new byte[10];
    Arrays.fill(bytes, (byte) 10);

    BasicClusterTest.withNamespace(dlns -> {
      try (var factory = new DLStreamFactory(dlns)) {
        var writeProxy = new GrpcService(
          writeServer,
          GrpcWriteProxy.server(factory, noSecurity())
        );
        var writeChannel = writeClient.build();
        var writer = new GrpcStreamWriter(
          writeChannel,
          Streamname,
          Flux.range(0, NumMessages).map(i ->
            Record.create(bytes, new TxId(i))
          ),
          SuperUserAuth
        );

        var readProxy = new GrpcService(
          readServer,
          GrpcReadProxy.server(factory, noSecurity())
        );
        var processor = new CountingProcessor();
        var readChannel = readClient.build();
        var reader = new GrpcStreamReader(
          readChannel,
          Streamname,
          Start.Newest,
          processor,
          SuperUserAuth
        );

        readProxy.startAsync().awaitRunning();
        writeProxy.startAsync().awaitRunning();
        writer.startAsync().awaitRunning();
        reader.startAsync().awaitRunning();

        awaitProcessed(processor, NumMessages);

        reader.stopAsync().awaitTerminated();
        writeProxy.stopAsync().awaitTerminated();
        readProxy.stopAsync().awaitTerminated();

        readChannel.shutdownNow();
        writeChannel.shutdownNow();
      }
    });
  }
}
