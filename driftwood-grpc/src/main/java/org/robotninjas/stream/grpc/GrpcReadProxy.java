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
import org.robotninjas.stream.proto.ReactorReadProxyGrpc;
import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.server.ReadProxySupport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GrpcReadProxy {
  public static ReactorReadProxyGrpc.ReadProxyImplBase server(StreamFactory streamFactory, Security security) {
    return new GrpcReadProxyFacade(new ReadProxySupport(streamFactory, security));
  }

  private static class GrpcReadProxyFacade extends ReactorReadProxyGrpc.ReadProxyImplBase {
    private final ReadProxySupport support;

    public GrpcReadProxyFacade(ReadProxySupport support) {
      this.support = support;
    }

    @Override
    public Flux<Read.Response> read(Mono<Read.Request> request) {
      // the only way we can pass these things between layers is in the context
      // but we contain that to the grpc side of things and kill the context
      // here without propagating it.
      var authToken = ContextKeys.AuthTokenContextKey.get();
      return support.read(request, new GrpcCallContext(authToken))
        .onErrorMap(ErrorMapper::toStatus);
    }
  }
}
