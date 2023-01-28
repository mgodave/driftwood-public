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

import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import org.junit.Test;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.dlog.DLStreamFactory;
import org.robotninjas.stream.rsocket.RSocketReadProxy;
import org.robotninjas.stream.rsocket.RSocketService;
import org.robotninjas.stream.rsocket.RSocketStreamReader;

public class RSocketReaderTest implements BasicClusterTest {
  @Test
  public void writeSomeStuffAndReadIt() throws Exception {
    final String baseName = RSocketReaderTest.class.getName();

    final LocalServerTransport serverTransport =
      LocalServerTransport.create(baseName + "_read");
    final LocalClientTransport clientTransport =
      LocalClientTransport.create(baseName + "_read");

    BasicClusterTest.withNamespace(dlns -> {
      try (var streamFactory = new DLStreamFactory(dlns)) {

        writeData(dlns, Streamname, NumMessages);

        var readProxy = new RSocketService(
          serverTransport,
          RSocketReadProxy.server(streamFactory, noSecurity())
        );
        readProxy.startAsync().awaitRunning();

        var processor = new CountingProcessor();
        var streamReader = new RSocketStreamReader(
          clientTransport,
          Streamname,
          Start.Newest,
          processor,
          SuperUserAuth
        ).startAsync();

        awaitProcessed(processor, NumMessages);

        streamReader.stopAsync().awaitTerminated();
        readProxy.stopAsync().awaitTerminated();
      }
    });
  }
}
