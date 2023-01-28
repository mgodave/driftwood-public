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

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Test;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.dlog.DLStreamFactory;
import org.robotninjas.stream.grpc.GrpcReadProxy;
import org.robotninjas.stream.grpc.GrpcService;
import org.robotninjas.stream.grpc.GrpcStreamReader;

public class GrpcReaderTest implements BasicClusterTest {
  @Test
  public void writeSomeStuffAndReadIt() throws Exception {
    final String baseName = GrpcReaderTest.class.getName();

    final ServerBuilder<? extends ServerBuilder<?>> server =
      InProcessServerBuilder.forName(baseName + "_read");
    final ManagedChannelBuilder<? extends ManagedChannelBuilder<?>> client =
      InProcessChannelBuilder.forName(baseName + "_read");

    BasicClusterTest.withNamespace(dlns -> {
      try (var streamFactory = new DLStreamFactory(dlns)) {
        writeData(dlns, Streamname, NumMessages);

        var readProxy = new GrpcService(
          server,
          GrpcReadProxy.server(streamFactory, noSecurity())
        );
        readProxy.startAsync().awaitRunning();

        var processor = new CountingProcessor();
        var channel = client.build();
        var streamReader = new GrpcStreamReader(
          channel,
          Streamname,
          Start.Newest,
          processor,
          SuperUserAuth
        ).startAsync();

        awaitProcessed(processor, NumMessages);

        streamReader.stopAsync().awaitTerminated();
        readProxy.stopAsync().awaitTerminated();
        channel.shutdownNow();
      }
    });
  }
}
