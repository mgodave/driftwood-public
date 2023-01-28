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
package org.robotninjas.stream.client;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.robotninjas.stream.MetricsNames;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.SeqId;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.TxId;
import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.proto.Start.FromNewest;
import org.robotninjas.stream.proto.Start.FromOffset;
import org.robotninjas.stream.proto.Start.FromOldest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;

final public class StreamReader {

  private final Function<Mono<Read.Request>, Flux<Read.Response>> readf;
  private final Counter messagesRecv = Metrics.counter(MetricsNames.ReaderReceived);

  private final CircuitBreaker circuitBreaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker("streamreader");
  private final Retry retry =
    RetryRegistry.ofDefaults().retry(
      "streamreader",
      RetryConfig.custom()
        .waitDuration(Duration.ofMillis(100))
        .intervalFunction(ofExponentialRandomBackoff())
        .build()
    );

  private StreamReader(Function<Mono<Read.Request>, Flux<Read.Response>> readf) {
    this.readf = readf;
    TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(CircuitBreakerRegistry.ofDefaults()).bindTo(Metrics.globalRegistry);
    TaggedRetryMetrics.ofRetryRegistry(RetryRegistry.ofDefaults()).bindTo(Metrics.globalRegistry);
  }

  private static org.robotninjas.stream.proto.Start getStart(Start start) {
    var builder = org.robotninjas.stream.proto.Start.newBuilder();

    return switch (start) {
      case Start.FromNewest ignore ->
        builder.setFromNewest(FromNewest.newBuilder()).build();
      case Start.FromOldest ignore ->
        builder.setFromOldest(FromOldest.newBuilder()).build();
      case Start.FromOffset fromOffset ->
        builder.setFromOffset(FromOffset.newBuilder()
          .setOffset(unsafeWrap(fromOffset.offset().bytes()))).build();
    };
  }

  public Flux<Record.Stored> read(String stream, Start start) {
    return readBatch(stream, start).flatMap(Flux::fromIterable);
  }

  private void countMessagesReceived(Read.Response response) {
    if (response.hasSuccess()) {
      messagesRecv.increment(response.getSuccess().getBatchCount());
    }
  }

  public Flux<List<Record.Stored>> readBatch(String stream, Start start) {
    var request = Read.Request.newBuilder()
      .setName(stream)
      .setStart(getStart(start));

    return Mono.just(request.build()).as(readf)
      .transform(CircuitBreakerOperator.of(circuitBreaker))
      .transform(RetryOperator.of(retry))
      .doOnNext(this::countMessagesReceived)
      .flatMap(StreamReader::toRecords);
  }

  private static Mono<List<Record.Stored>> toRecords(Read.Response response) {
    return switch (response.getResponseCase()) {
      case SUCCESS -> {
        Read.Success success = response.getSuccess();
        List<Record.Stored> records = new ArrayList<>(success.getBatchCount());
        for (Read.Record readRecord : success.getBatchList()) {
          records.add(new ReadRecord(readRecord));
        }
        yield Mono.just(records);
      }
      case FAILURE -> Mono.error(new Exception("unknown exception"));
      case RESPONSE_NOT_SET -> Mono.error(
        new IllegalStateException("neither success nor failure specified in response")
      );
    };
  }

  public static StreamReader from(Function<Read.Request, Flux<Read.Response>> f) {
    return new StreamReader(request -> request.flatMapMany(f));
  }

  private record ReadRecord(Read.Record event) implements Record.Stored {
    @Override
    public Offset offset() {
      return Offset.from(event.getOffset().toByteArray());
    }

    @Override
    public ByteBuffer data() {
      return event.getData().asReadOnlyByteBuffer();
    }

    @Override
    public SeqId seqId() {
      return new SeqId(event.getSeq());
    }

    @Override
    public TxId txid() {
      return new TxId(event.getTxid());
    }
  }
}
