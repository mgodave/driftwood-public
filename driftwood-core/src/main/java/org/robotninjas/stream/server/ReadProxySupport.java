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
package org.robotninjas.stream.server;

import org.reactivestreams.Publisher;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.*;
import org.robotninjas.stream.filters.MetricsFilter;
import org.robotninjas.stream.filters.MetricsFilter.Tags;
import org.robotninjas.stream.proto.BackPressureStrategy;
import org.robotninjas.stream.proto.BatchingStrategy;
import org.robotninjas.stream.proto.OverflowStrategy;
import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Permission;
import org.robotninjas.stream.security.Permissions;
import org.robotninjas.stream.security.Security;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

@ParametersAreNonnullByDefault
final public class ReadProxySupport {
  private static final Logger Logger = LoggerFactory.getLogger(ReadProxySupport.class);

  static final int MaxBufferPerStream = 1000;

  static final int MaxBufferPerResponse = 100;

  static final Duration DefaultBufferTime = Duration.ofMillis(10);

  private final StreamFactory logStreamFactory;
  private final Security security;

  public ReadProxySupport(StreamFactory logStreamFactory, Security security) {
    this.logStreamFactory = Objects.requireNonNull(logStreamFactory, "logStreamFactory must be nonnull");
    this.security = Objects.requireNonNull(security, "security must be nonnull");
  }

  public Flux<Read.Response> read(Mono<Read.Request> mono, CallContext ctx) {
    return mono.flatMapMany(request -> read(request, ctx));
  }

  public Flux<Read.Response> read(Read.Request request, CallContext ctx) {
    //TODO Instead of throwing these return Flux.error(...);
    Objects.requireNonNull(request, "request must be nonnull");
    Objects.requireNonNull(ctx, "ctx must be nonnull");

    var stream = StreamName.of(request.getName());

    var start = request.hasStart()
      ? GetStart.fromProto(request.getStart())
      : org.robotninjas.stream.Start.Newest;

    Logger.info("Stream: {}, Start: {}", stream, start);

    return checkPermissions(ctx, Permissions.read(stream), () ->
      logStreamFactory.get(stream)
        .read(start)
        .name("readproxy_" + request.getName())
        .transform(backpressure(request.getBackPressureStrategy()))
        .transform(batching(request.getBatchingStrategy()))
        .map(ReadProxySupport::toResponse)
        .transform(MetricsFilter.op(Tags.read(stream)))
        .doOnError(t -> Logger.info("Error while reading stream", t))
        .transform(context(request))
    );
  }

  private <T> Flux<T> checkPermissions(CallContext ctx, Permission perm, Supplier<Flux<T>> supplier) {
    return security.login(ctx).flatMapMany(principal ->
      principal.check(perm).flatMapMany(ignore ->
        supplier.get().transform(Security.op(principal, perm))
      )
    );
  }

  private static Read.Response toResponse(List<Record.Stored> stored) {
    return Read.Response.newBuilder()
      .setSuccess(Read.Success.newBuilder()
        .addAllBatch(ToReadRecord.batched(stored))
      ).build();
  }

  static <T> BackPressure<T> backpressure(BackPressureStrategy strategy) {
    return switch(strategy.getStrategyCase()) {
      case BUFFER -> {
        var maxSize = strategy.getBuffer().getMaxSize();
        //TODO set a limit on maxSize so we don'cause buffer infinitely
        var overflow = overflowStrategy(strategy.getBuffer().getOverflowStrategy());
        yield BackPressure.buffer(maxSize, overflow);
      }
      case ERROR -> BackPressure.error();
      case LATEST -> BackPressure.latest();
      case DROP -> BackPressure.drop();
      case STRATEGY_NOT_SET -> BackPressure.none();
    };
  }

  static BufferOverflowStrategy overflowStrategy(OverflowStrategy strategy) {
    return switch (strategy) {
      case DROP_LATEST -> BufferOverflowStrategy.DROP_OLDEST;
      case DROP_NEWEST -> BufferOverflowStrategy.DROP_LATEST;
      case ERROR, UNRECOGNIZED -> BufferOverflowStrategy.ERROR;
    };
  }

  static <T> Batching<T> batching(BatchingStrategy strategy) {
    var time = Math.min(DefaultBufferTime.toMillis(), strategy.getMaxTimeMs());
    var maxSize = Math.min(MaxBufferPerStream, strategy.getMaxSize());

    if (time > 0 && maxSize > 0) {
      return Batching.bySizeAndTime(maxSize, Duration.ofMillis(time));
    } else if (time > 0) {
      return Batching.byTime(Duration.ofMillis(time));
    } else if (maxSize > 0){
      return Batching.bySize(maxSize);
    } else {
      return Batching.bySizeAndTime(MaxBufferPerResponse, DefaultBufferTime);
    }
  }

  static <T> Function<Publisher<T>, Publisher<T>> context(Read.Request request) {
    return response -> Flux.from(response).contextWrite(ctx ->
      ctx.putNonNull(ContextKeys.StreamName, request.getName())
        .putNonNull(ContextKeys.StartPoint, request.getStart())
        .putNonNull(ContextKeys.Operation, "read")
    );
  }

}
