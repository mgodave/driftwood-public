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
package org.robotninjas.stream.examples;

import com.google.common.util.concurrent.ServiceManager;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.grpc.GrpcTransfer;
import org.robotninjas.stream.proto.ReactorReplicatorGrpc;
import org.robotninjas.stream.test.MockStreamFactory;
import org.robotninjas.stream.transfer.StreamFactoryTransfer;
import org.robotninjas.stream.transfer.Transfer;
import org.robotninjas.stream.transfer.TransferService;
import reactor.core.publisher.Flux;

import java.util.List;

@SuppressWarnings("UnusedVariable")
public class TransferExamples {
   private static final StreamName FromStream = StreamName.of("from");
   private static final StreamName ToStream = StreamName.of("to");
   private static final Start StartingPoint = Start.Newest;

   private static final ManagedChannel channel =
      InProcessChannelBuilder.forName("foo").build();

   public static void main(String[] args) {
      StreamFactory factory = new MockStreamFactory(Flux.empty());

      ReactorReplicatorGrpc.ReactorReplicatorStub stub =
         ReactorReplicatorGrpc.newReactorStub(channel);

      var services = new ServiceManager(List.of(
         // local to local, replicate to another stream in the same cluster
         new TransferService(
            Transfer.create(
               StreamFactoryTransfer.source(factory),
               StreamFactoryTransfer.sink(factory)
            ),
            FromStream, StartingPoint, ToStream
         ),

         // gRPC to gRPC, bridge two gRPC clusters
         new TransferService(
            Transfer.create(
               GrpcTransfer.source(stub),
               GrpcTransfer.sink(stub)
            ),
            FromStream, StartingPoint, ToStream
         ),

         // gRPC to local, pull from a remote gRPC cluster
         new TransferService(
            Transfer.create(
               GrpcTransfer.source(stub),
               StreamFactoryTransfer.sink(factory)
            ),
            FromStream, StartingPoint, ToStream
         ),

         // local to local, transform to another
         // stream in the same cluster.
         new TransferService(
            Transfer.create(
              StreamFactoryTransfer.source(factory),
              StreamFactoryTransfer.sink(factory)
            ),
            FromStream, StartingPoint, ToStream
         ),

         new TransferService(
            Transfer.create(
               GrpcTransfer.source(stub),
               GrpcTransfer.sink(stub)
            ),
            FromStream, StartingPoint, ToStream
         )

      ));

      services.startAsync().awaitStopped();
   }
}