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
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialRandomBackoff;

class ManagerCache implements AutoCloseable {
  private static final Logger Log = LoggerFactory.getLogger(ManagerCache.class);

  private static final RetryRegistry retryRegistry = RetryRegistry.ofDefaults();

  private final Retry LogManagerRetry =
    retryRegistry.retry(
      "asynclogwriter",
      RetryConfig.custom()
        .waitDuration(Duration.ofMillis(100))
        .ignoreExceptions(
          InvalidStreamNameException.class,
          LogNotFoundException.class
        )
        .intervalFunction(ofExponentialRandomBackoff())
        .maxAttempts(10)
        .build()
    );

  private final LoadingCache<String, DistributedLogManager> managers;

  ManagerCache(Namespace dlns) {
    this.managers = Caffeine.newBuilder().weakValues()
      .removalListener((String key, DistributedLogManager mgr, RemovalCause cause) -> {
        if (mgr != null) {
          Log.info("Closing manager for {}: {}", mgr.getStreamName(), cause);
          Mono.fromCompletionStage(mgr.asyncClose()).block();
        }
      }).recordStats().build(dlns::openLog);

    CaffeineCacheMetrics.monitor(Metrics.globalRegistry, managers, "manager_cache");
    TaggedRetryMetrics.ofRetryRegistry(retryRegistry).bindTo(Metrics.globalRegistry);
  }

  <T> Flux<T> withManager(String stream, Function<DistributedLogManager, Publisher<T>> f) {
    return Flux.usingWhen(
      Mono.fromCallable(() -> managers.get(stream)),
      f,
      mgr -> Mono.empty(),
      (mgr, err) -> Mono.fromRunnable(() ->
        managers.invalidate(stream)
      ),
      mgr -> Mono.empty()
    ).transform(RetryOperator.of(LogManagerRetry));
  }

  @Override
  public void close() {
    managers.invalidateAll();
  }
}
