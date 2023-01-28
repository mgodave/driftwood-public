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

import org.robotninjas.stream.grpc.GrpcTransfer;
import org.robotninjas.stream.replication.ReplicationService;
import org.robotninjas.stream.transfer.StreamFactoryTransfer;
import org.robotninjas.stream.transfer.Transfer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import reactor.core.publisher.Flux;

@SuppressWarnings("unused")
@Command(name = "grpc", abbreviateSynopsis = true)
public class GrpcReplicatorCommand extends BaseServerCommand implements Callable<Integer> {
   @Mixin
   private GrpcReplicationOptions replicationOptions;

   public GrpcReplicatorCommand(GrpcReplicationOptions replicationOptions) {
      this.replicationOptions = replicationOptions;
   }

   @Override
   public Integer call() throws Exception {
      runServiceWithStreamFactory(streamFactory ->
         replicationOptions.withChannel( channel ->
            new ReplicationService(Flux::empty, () ->
               Transfer.create(
                  StreamFactoryTransfer.source(streamFactory),
                  GrpcTransfer.sink(channel)
               )
            )
         )
      );
      return 0;
   }
}
