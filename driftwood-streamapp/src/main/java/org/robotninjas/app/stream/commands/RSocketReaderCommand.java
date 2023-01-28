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
package org.robotninjas.app.stream.commands;

import java.util.concurrent.Callable;

import io.rsocket.transport.netty.client.TcpClientTransport;
import org.robotninjas.stream.rsocket.RSocketStreamReader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@SuppressWarnings("unused")
@Command(name = "rsocket", abbreviateSynopsis = true)
public class RSocketReaderCommand extends RSocketClientCommand implements Callable<Integer> {
  @Mixin
  private ReaderOptions readerOptions = new ReaderOptions();

  public void readerOptions(ReaderOptions readerOptions) {
    this.readerOptions = readerOptions;
  }

  @Override
  public Integer call() {
    runServiceAsApplication(() -> {
      var transport = TcpClientTransport.create(tcpClient());
      return new RSocketStreamReader(
        transport,
        readerOptions.streamName(),
        readerOptions.start(),
        readerOptions.processor(),
        readerOptions.auth().token()
      );
    });
    return 0;
  }
}
