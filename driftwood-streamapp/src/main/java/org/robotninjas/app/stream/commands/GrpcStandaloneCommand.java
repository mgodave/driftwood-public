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

import org.robotninjas.stream.grpc.GrpcReadProxy;
import org.robotninjas.stream.grpc.GrpcReplicator;
import org.robotninjas.stream.grpc.GrpcService;
import org.robotninjas.stream.grpc.GrpcWriteProxy;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "grpc", abbreviateSynopsis = true)
public class GrpcStandaloneCommand extends BaseServerCommand implements Callable<Integer> {
  @SuppressWarnings("UnusedAssignment")
  @Mixin
  private GrpcServerOptions serverOptions = new GrpcServerOptions();

  @SuppressWarnings("unused")
  public GrpcStandaloneCommand() { }

  @SuppressWarnings("unused")
  public GrpcStandaloneCommand(GrpcServerOptions serverOptions) {
    this.serverOptions = serverOptions;
  }

  @Override
  public Integer call() throws Exception {
    runStandaloneWithStreamFactory(streamFactory ->
      serverOptions.withServerBuilder(server ->
        new GrpcService(server,
          GrpcReplicator.server(streamFactory, security()),
          GrpcReadProxy.server(streamFactory, security()),
          GrpcWriteProxy.server(streamFactory, security())
        )
      )
    );
    return 0;
  }
}
