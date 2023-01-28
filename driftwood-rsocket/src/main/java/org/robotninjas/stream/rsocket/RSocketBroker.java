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
import org.robotninjas.stream.admin.AdminRepository;
import org.robotninjas.stream.broker.AckTracker;
import org.robotninjas.stream.broker.AckTrackerFactory;
import org.robotninjas.stream.broker.BrokerSupport;
import org.robotninjas.stream.broker.OffsetStore;
import org.robotninjas.stream.cluster.ResourceLocator;
import org.robotninjas.stream.proto.Acknowledge;
import org.robotninjas.stream.proto.Broker;
import org.robotninjas.stream.proto.BrokerServer;
import org.robotninjas.stream.proto.Publish;
import org.robotninjas.stream.proto.Subscribe;
import org.robotninjas.stream.security.Security;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketBroker {
  private static final CompositeMetadataDecoder MetadataDecoder = new CompositeMetadataDecoder();

  private static record RSocketBrokerFacade(BrokerSupport support) implements Broker {
    @Override
    public Flux<Subscribe.Response> subscribe(Subscribe.Request request, ByteBuf metadata) {
      return support.subscribe(Mono.just(request), RSocketCallContext.fromMetadata(new CompositeMetadata(metadata, false)));
    }

    @Override
    public Flux<Publish.Response> publish(Publisher<Publish.Request> request, ByteBuf metadata) {
      return support.publish(Flux.from(request), RSocketCallContext.fromMetadata(new CompositeMetadata(metadata, false)));
    }

    @Override
    public Flux<Acknowledge.Response> acknowledge(Publisher<Acknowledge.Request> request, ByteBuf metadata) {
      return support.acknowledge(Flux.from(request), RSocketCallContext.fromMetadata(new CompositeMetadata(metadata, false)));
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static BrokerServer server(
    StreamFactory streamFactory,
    ResourceLocator resourceLocator,
    AdminRepository adminRepository,
    Security security,
    Optional<MeterRegistry> metrics,
    Optional<Tracer> tracer
  ) {
    return new BrokerServer(
      new RSocketBroker.RSocketBrokerFacade(
        new BrokerSupport(
          streamFactory,
          resourceLocator,
          adminRepository,
          OffsetStore.NullStore,
          AckTrackerFactory.Null,
          security
        )
      ),
      Optional.of(MetadataDecoder),
      metrics,
      tracer
    );
  }

  public static BrokerServer server(
    StreamFactory streamFactory,
    ResourceLocator resourceLocator,
    AdminRepository adminRepository,
    Security security,
    MeterRegistry metrics,
    Tracer tracer
  ) {
    return server(streamFactory, resourceLocator, adminRepository, security, Optional.of(metrics), Optional.of(tracer));
  }

  public static BrokerServer server(
    StreamFactory streamFactory,
    ResourceLocator resourceLocator,
    AdminRepository adminRepository,
    Security security
  ) {
    return server(streamFactory, resourceLocator, adminRepository, security, Optional.of(Metrics.globalRegistry), Optional.empty());
  }
}
