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
import org.robotninjas.app.ServiceApplication;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import reactor.netty.tcp.TcpClient;

@SuppressWarnings("unused")
public class RSocketClientCommand extends ServiceApplication {
  private TcpClient tcpClient = TcpClient.create();

  @Option(names = {"--address"}, defaultValue = "127.0.0.1:7001")
  void address(HostAndPort address) {
    tcpClient = tcpClient.remoteAddress(() ->
      new InetSocketAddress(address.getHost(), address.getPort())
    );
  }

  @ArgGroup(exclusive = false)
  void tlsOptions(TlsOptions tlsOptions) {
    tcpClient = tcpClient.secure(sslContextSpec ->
      SslContextBuilder.forServer(
        tlsOptions.certChain(),
        tlsOptions.privateKey()
      )
    );
  }

  protected TcpClient tcpClient() {
    return tcpClient;
  }
}