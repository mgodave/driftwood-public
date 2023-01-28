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
package org.robotninjas.stream.rsocket;

import java.util.Optional;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.netty.buffer.ByteBuf;
import io.opentracing.Tracer;
import io.rsocket.ipc.decoders.CompositeMetadataDecoder;
import io.rsocket.metadata.CompositeMetadata;
import org.reactivestreams.Publisher;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.proto.MarkEnd;
import org.robotninjas.stream.proto.Truncate;
import org.robotninjas.stream.proto.Write;
import org.robotninjas.stream.proto.WriteProxy;
import org.robotninjas.stream.proto.WriteProxyServer;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.server.WriteProxySupport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketWriteProxy {
  private static final CompositeMetadataDecoder MetadataDecoder = new CompositeMetadataDecoder();

  private static CallContext callContext(ByteBuf metadata) {
    return RSocketCallContext.fromMetadata(new CompositeMetadata(metadata, false));
  }

  private record RSocketWriteProxyFacade(WriteProxySupport support) implements WriteProxy {
    @Override
    public Flux<Write.Response> write(Publisher<Write.Request> messages, ByteBuf metadata) {
      return support.write(Flux.from(messages), callContext(metadata));
    }

    @Override
    public Mono<Truncate.Response> truncate(Truncate.Request message, ByteBuf metadata) {
      return support.truncate(Mono.just(message), callContext(metadata));
    }

    @Override
    public Mono<MarkEnd.Response> markEnd(MarkEnd.Request message, ByteBuf metadata) {
      return support.markEnd(Mono.just(message), callContext(metadata));
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static WriteProxyServer server(StreamFactory streamFactory, Security security, Optional<MeterRegistry> metrics, Optional<Tracer> tracer) {
    return new WriteProxyServer(new RSocketWriteProxyFacade(new WriteProxySupport(streamFactory, security)), Optional.of(MetadataDecoder), metrics, tracer);
  }

  public static WriteProxyServer server(StreamFactory streamFactory, Security security, MeterRegistry metrics, Tracer tracer) {
    return server(streamFactory, security, Optional.of(metrics), Optional.of(tracer));
  }

  public static WriteProxyServer server(StreamFactory streamFactory, Security security) {
    return server(streamFactory, security, Optional.of(Metrics.globalRegistry), Optional.empty());
  }

}
