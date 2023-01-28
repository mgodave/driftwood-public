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
package org.robotninjas.stream.dlog;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.ReadCancelledException;
import org.robotninjas.stream.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Adapt an {@link AsyncLogReader} to a {@link Flux} based API
 */
@FunctionalInterface
interface StreamReader {
  Flux<Record.Stored> read(DLSN dlsn);

  Logger LOGGER = LoggerFactory.getLogger(StreamReader.class);

  RetryRegistry retryRegistry = RetryRegistry.ofDefaults();

  Retry ReadRetry =
    retryRegistry.retry(
      "asynclogwriter",
      RetryConfig.custom()
        .waitDuration(Duration.ofMillis(100))
        .retryExceptions(LogEmptyException.class)
        .intervalFunction(ofExponentialRandomBackoff())
        .build()
    );

  private static Flux<Record.Stored> readRecords(AsyncLogReader rdr) {
    return Flux.<CompletableFuture<List<LogRecordWithDLSN>>>create(emitter ->
      emitter.onRequest(n -> emitter.next(
        rdr.readBulk(Long.valueOf(n).intValue(), 5, SECONDS)
      ))
    )
    .flatMapSequential(Mono::fromCompletionStage)
    .doOnError(t -> LOGGER.debug("Error Reading", t))
    .transform(RetryOperator.of(ReadRetry))
    .flatMap(Flux::fromIterable)
    .map(DLRecord::new);
  }

  static StreamReader create(DistributedLogManager mgr) {
//    TaggedRetryMetrics.ofRetryRegistry(retryRegistry).bindTo(Metrics.globalRegistry);
    return (DLSN dlsn) ->
      Flux.usingWhen(
          Mono.fromCallable(() -> mgr.getAsyncLogReader(dlsn)),
          StreamReader::readRecords,
          rdr -> Mono.fromCompletionStage(rdr.asyncClose())
            // ReadCancelledException occurs when the connection is closed, ignore
            .onErrorResume(ReadCancelledException.class, ignore -> Mono.empty())
      );
  }
}
