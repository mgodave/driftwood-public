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
import org.robotninjas.stream.proto.MarkEnd;
import org.robotninjas.stream.proto.ReactorWriteProxyGrpc;
import org.robotninjas.stream.proto.Truncate;
import org.robotninjas.stream.proto.Write;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.server.WriteProxySupport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GrpcWriteProxy extends ReactorWriteProxyGrpc.WriteProxyImplBase {
  public static ReactorWriteProxyGrpc.WriteProxyImplBase server(StreamFactory streamFactory, Security security) {
    return new GrpcWriteProxyFacade(new WriteProxySupport(streamFactory, security));
  }

  private static class GrpcWriteProxyFacade extends ReactorWriteProxyGrpc.WriteProxyImplBase {
    private final WriteProxySupport support;

    public GrpcWriteProxyFacade(WriteProxySupport support) {
      this.support = support;
    }

    private CallContext callContext() {
      // extract auth tokens from the Grpc Context an put it somewhere sane.
      var authToken = ContextKeys.AuthTokenContextKey.get();
      return new GrpcCallContext(authToken);
    }

    @Override
    public Flux<Write.Response> write(Flux<Write.Request> request) {
      return support.write(request, callContext())
        .onErrorMap(ErrorMapper::toStatus);
    }

    @Override
    public Mono<Truncate.Response> truncate(Mono<Truncate.Request> request) {
      return support.truncate(request, callContext())
        .onErrorMap(ErrorMapper::toStatus);
    }

    @Override
    public Mono<MarkEnd.Response> markEnd(Mono<MarkEnd.Request> request) {
      return support.markEnd(request, callContext())
        .onErrorMap(ErrorMapper::toStatus);
    }
  }
}
