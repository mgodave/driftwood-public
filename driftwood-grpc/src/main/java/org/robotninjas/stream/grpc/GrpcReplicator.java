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
package org.robotninjas.stream.grpc;

import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.proto.ReactorReplicatorGrpc;
import org.robotninjas.stream.proto.Replicate;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.transfer.TransferSupport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GrpcReplicator extends ReactorReplicatorGrpc.ReplicatorImplBase {
  private static class GrpcReplicatorFacade extends ReactorReplicatorGrpc.ReplicatorImplBase {
    private final TransferSupport support;

    private CallContext callContext() {
      // extract auth tokens from the Grpc Context an put it somewhere sane.
      var authToken = ContextKeys.AuthTokenContextKey.get();
      return new GrpcCallContext(authToken);
    }

    public GrpcReplicatorFacade(TransferSupport support) {
      this.support = support;
    }

    @Override
    public Flux<Replicate.Transfer> read(Mono<Replicate.Request> request) {
      return support.read(request, callContext()).onErrorMap(ErrorMapper::toStatus);
    }

    @Override
    public Flux<Replicate.Response> write(Flux<Replicate.Transfer> request) {
      return support.write(request, callContext()).onErrorMap(ErrorMapper::toStatus);
    }
  }

  public static ReactorReplicatorGrpc.ReplicatorImplBase server(StreamFactory streamFactory, Security security) {
    return new GrpcReplicatorFacade(new TransferSupport(streamFactory, security));
  }
}
