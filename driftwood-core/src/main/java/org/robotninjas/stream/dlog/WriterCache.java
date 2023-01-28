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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;

class WriterCache implements AutoCloseable {
  private static final Logger Log = LoggerFactory.getLogger(WriterCache.class);

  private final RetryRegistry retryRegistry = RetryRegistry.ofDefaults();

  private final Retry LogWriterRetry =
    retryRegistry.retry(
      "asynclogwriter",
      RetryConfig.custom()
        .waitDuration(Duration.ofMillis(100))
        .intervalFunction(ofExponentialRandomBackoff())
        .maxAttempts(10)
        .build()
    );

  private final ManagerCache managerCache;
  private final Cache<String, CompletableFuture<AsyncLogWriter>> writers;

  @SuppressWarnings("FutureReturnValueIgnored")
  WriterCache(ManagerCache managerCache) {
    this.managerCache = managerCache;
    this.writers = Caffeine.newBuilder().softValues()
      .removalListener((String key, CompletableFuture<AsyncLogWriter> writer, RemovalCause cause) -> {
        if (writer != null) {
          writer.thenAccept(w -> {
            Log.info("Closing writer for {}: {}", w.getStreamName(), cause);
            Mono.fromCompletionStage(w.asyncClose()).block();
          });
        }
      }).recordStats().build();

    CaffeineCacheMetrics.monitor(Metrics.globalRegistry, writers, "writer_cache");
    TaggedRetryMetrics.ofRetryRegistry(retryRegistry).bindTo(Metrics.globalRegistry);
  }

  <T> Flux<T> withWriter(String stream, Function<AsyncLogWriter, Publisher<T>> f) {
    return managerCache.withManager(stream, mgr ->
      Flux.usingWhen(
        Mono.fromCompletionStage(() -> writers.get(
          stream, k -> mgr.openAsyncLogWriter()
        )),
        f,
        writer -> Mono.empty()
      )
    ).transform(RetryOperator.of(LogWriterRetry));
  }

  @Override
  public void close() {
    writers.invalidateAll();
  }
}
