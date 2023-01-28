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

import java.net.InetSocketAddress;

import com.google.common.net.HostAndPort;
import io.netty.handler.ssl.SslContextBuilder;
import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.robotninjas.app.HostAndPortConverter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import reactor.netty.tcp.TcpServer;

@SuppressWarnings("unused")
class RSocketServerCommand extends BaseServerCommand {
  private TcpServer tcpServer = TcpServer.create();

  @Option(names = {"--listen-address"}, required = true, converter = {HostAndPortConverter.class})
  void listenAddress(HostAndPort listenAddress) {
    tcpServer = tcpServer.bindAddress(() ->
      new InetSocketAddress(listenAddress.getHost(), listenAddress.getPort())
    );
  }

  @ArgGroup(exclusive = false)
  void tlsOptions(TlsOptions tlsOptions) {
    tcpServer = tcpServer.secure(sslContextSpec ->
      SslContextBuilder.forServer(
        tlsOptions.certChain(),
        tlsOptions.privateKey()
      )
    );
  }

  /**
   * Exposed for testing so we can use the local transport
   */
  protected ServerTransport<? extends Closeable> transport() {
    return TcpServerTransport.create(tcpServer);
  }
}
