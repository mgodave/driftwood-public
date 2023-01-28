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

import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import org.junit.Test;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.TxId;
import org.robotninjas.stream.dlog.DLStreamFactory;
import org.robotninjas.stream.rsocket.RSocketReadProxy;
import org.robotninjas.stream.rsocket.RSocketService;
import org.robotninjas.stream.rsocket.RSocketStreamReader;
import org.robotninjas.stream.rsocket.RSocketStreamWriter;
import org.robotninjas.stream.rsocket.RSocketWriteProxy;
import reactor.core.publisher.Flux;

public class RSocketEndToEndTest implements BasicClusterTest {
  @Test
  public void writeSomeStuffAndReadIt() throws Exception {
    final String baseName = RSocketEndToEndTest.class.getName();

    final LocalServerTransport readServer =
      LocalServerTransport.create(baseName + "_read");
    final LocalClientTransport readClient =
      LocalClientTransport.create(baseName + "_read");

    final LocalServerTransport writeServer =
      LocalServerTransport.create(baseName + "_write");
    final LocalClientTransport writeClient =
      LocalClientTransport.create(baseName + "_write");

    var bytes = new byte[10];
    Arrays.fill(bytes, (byte) 10);

    BasicClusterTest.withNamespace(dlns -> {
      try (var factory = new DLStreamFactory(dlns)) {
        var writeProxy = new RSocketService(
          writeServer,
          RSocketWriteProxy.server(factory, noSecurity())
        );
        var writer = new RSocketStreamWriter(
          writeClient,
          Streamname,
          Flux.range(0, NumMessages).map(i ->
            Record.create(bytes, new TxId(i))
          ),
          SuperUserAuth
        );

        var readProxy = new RSocketService(
          readServer,
          RSocketReadProxy.server(factory, noSecurity())
        );
        var processor = new CountingProcessor();
        var reader = new RSocketStreamReader(
          readClient,
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
      }
    });
  }
}
