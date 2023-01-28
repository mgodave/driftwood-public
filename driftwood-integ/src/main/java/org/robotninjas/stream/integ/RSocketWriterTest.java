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
import org.robotninjas.app.dl.DLReader;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.TxId;
import org.robotninjas.stream.dlog.DLStreamFactory;
import org.robotninjas.stream.rsocket.RSocketService;
import org.robotninjas.stream.rsocket.RSocketStreamWriter;
import org.robotninjas.stream.rsocket.RSocketWriteProxy;
import reactor.core.publisher.Flux;

public class RSocketWriterTest implements BasicClusterTest {
  @Test
  public void writeSomeStuffAndReadIt() throws Exception {
    final String baseName = RSocketWriterTest.class.getName();

    final LocalServerTransport serverTransport =
      LocalServerTransport.create(baseName + "_write");
    final LocalClientTransport clientTransport =
      LocalClientTransport.create(baseName + "_write");

    var bytes = new byte[10];
    Arrays.fill(bytes, (byte) 10);

    BasicClusterTest.withNamespace(dlns -> {
      try (var factory = new DLStreamFactory(dlns)) {
        var writeProxy = new RSocketService(
          serverTransport,
          RSocketWriteProxy.server(factory, noSecurity())
        );
        var writer = new RSocketStreamWriter(
          clientTransport,
          Streamname,
          Flux.range(0, NumMessages).map( i ->
            Record.create(bytes, new TxId(i))
          ),
          SuperUserAuth
        );

        var countingConsumer = new CountingConsumer();
        var reader = new DLReader(factory, Streamname, countingConsumer);

        writeProxy.startAsync().awaitRunning();
        writer.startAsync().awaitTerminated();
        reader.startAsync().awaitRunning();

        awaitConsumed(countingConsumer, NumMessages);

        writeProxy.stopAsync().awaitTerminated();
        reader.stopAsync().awaitTerminated();
      }
    });
  }
}
