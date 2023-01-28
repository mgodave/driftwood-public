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
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.proto.ReadProxy;
import org.robotninjas.stream.proto.ReadProxyServer;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.server.ReadProxySupport;
import reactor.core.publisher.Flux;

public class RSocketReadProxy {
  private static final CompositeMetadataDecoder MetadataDecoder = new CompositeMetadataDecoder();

  private static record RSocketReadProxyFacade(ReadProxySupport support) implements ReadProxy {
    @Override
    public Flux<Read.Response> read(Read.Request request, ByteBuf metadata) {
      return support.read(request, RSocketCallContext.fromMetadata(new CompositeMetadata(metadata, false)));
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static ReadProxyServer server(StreamFactory streamFactory, Security security, Optional<MeterRegistry> metrics, Optional<Tracer> tracer) {
    return new ReadProxyServer(new RSocketReadProxyFacade(new ReadProxySupport(streamFactory, security)), Optional.of(MetadataDecoder), metrics, tracer);
  }

  public static ReadProxyServer server(StreamFactory streamFactory, Security security, MeterRegistry metrics, Tracer tracer) {
    return server(streamFactory, security, Optional.of(metrics), Optional.of(tracer));
  }

  public static ReadProxyServer server(StreamFactory streamFactory, Security security) {
    return server(streamFactory, security, Optional.of(Metrics.globalRegistry), Optional.empty());
  }
}
