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
package org.robotninjas.stream.broker;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.robotninjas.stream.BackPressure;
import org.robotninjas.stream.Batching;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.SubscriberName;
import org.robotninjas.stream.ToReadRecord;
import org.robotninjas.stream.admin.AdminRepository;
import org.robotninjas.stream.cluster.ResourceLocator;
import org.robotninjas.stream.filters.MetricsFilter;
import org.robotninjas.stream.filters.MetricsFilter.Tags;
import org.robotninjas.stream.proto.Acknowledge;
import org.robotninjas.stream.proto.Publish;
import org.robotninjas.stream.proto.Subscribe;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Permissions;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.server.ContextKeys;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

public class BrokerSupport implements Broker {
  private static final int MaxBufferPerStream = 1000;

  private static final int MaxBufferPerResponse = 100;

  private static final Batching<Record.Stored> BufferRecordsBySizeAndTime =
    Batching.bySizeAndTime(MaxBufferPerResponse, Duration.ofMillis(10));

  private static final BackPressure<Record.Stored> OnBackPressureBufferThenError =
    BackPressure.buffer(MaxBufferPerStream, BufferOverflowStrategy.ERROR);

  private final StreamFactory streamFactory;
  private final ResourceLocator resourceLocator;
  private final AdminRepository adminRepository;
  private final OffsetStore offsetTracker;
  private final AckTrackerFactory ackTrackerFactory;
  private final Security security;

  public BrokerSupport(
    StreamFactory streamFactory,
    ResourceLocator resourceLocator,
    AdminRepository adminRepository,
    OffsetStore offsetTracker,
    AckTrackerFactory ackTrackerFactory,
    Security security
  ) {
    this.streamFactory = streamFactory;
    this.resourceLocator = resourceLocator;
    this.adminRepository = adminRepository;
    this.offsetTracker = offsetTracker;
    this.ackTrackerFactory = ackTrackerFactory;
    this.security = security;
  }

  private static Subscribe.Response toSubscribeResponse(List<Record.Stored> stored) {
    return Subscribe.Response.newBuilder()
      .addAllRecords(ToReadRecord.batched(stored))
      .build();
  }

  @Override
  public Flux<Subscribe.Response> subscribe(Mono<Subscribe.Request> requests, CallContext ctx) {
    return requests.flatMapMany(request -> {
      var subscriber = SubscriberName.of(requireNonNull(request.getSubscriber()));
      return security.login(ctx).flatMapMany(subject -> {
        subject.check(Permissions.subscribe(subscriber));

        return adminRepository.getSubscriber(subscriber).flatMapMany(subscriberInfo -> {

          var ackTracker = ackTrackerFactory.get(subscriber);

          return offsetTracker.get(subscriber).flatMapMany(offset ->
            streamFactory.get(subscriberInfo.stream())
              .read(new Start.FromOffset(offset))
              .name("broker" + request.getSubscriber())
              .mergeWith(ackTracker.retries())
              .transform(ackTracker.track())
              .transform(OnBackPressureBufferThenError)
              .transform(BufferRecordsBySizeAndTime)
              .map(BrokerSupport::toSubscribeResponse)
              .transform(MetricsFilter.op(Tags.subscribe(subscriber)))
              .transform(WithContext.op(request))
          );
        });
      });
    });
  }

  @Override
  public Flux<Publish.Response> publish(Flux<Publish.Request> requests, CallContext ctx) {
    return Flux.empty();
  }

  @Override
  public Flux<Acknowledge.Response> acknowledge(Flux<Acknowledge.Request> request, CallContext ctx) {
    return Flux.empty();
  }

  @FunctionalInterface
  interface WithContext extends Function<Flux<Subscribe.Response>, Publisher<Subscribe.Response>> {
    static WithContext op(Subscribe.Request request) {
      return (Flux<Subscribe.Response> readResponseFlux) -> readResponseFlux.subscriberContext(ctx ->
        ctx.putNonNull(ContextKeys.Subscriber, request.getSubscriber())
      );
    }
  }

}
