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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import io.micrometer.core.instrument.Metrics;
import org.apache.distributedlog.api.DistributedLogManager;
import org.reactivestreams.Publisher;
import org.robotninjas.stream.MetricsNames;
import org.robotninjas.stream.Record;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@FunctionalInterface
interface LagRecorder extends Function<Flux<Record.Stored>, Publisher<Record.Stored>> {
  static LagRecorder operator(DistributedLogManager mgr) {
    return tFlux -> {
      AtomicLong lag = Metrics.gauge(MetricsNames.ReadProxyLag, Tags.get(tFlux), new AtomicLong(0));

      Flux<Long> lastTxId = Flux.interval(Duration.ofSeconds(5)).flatMapSequential(i ->
        Mono.fromCompletionStage(mgr::getLastTxIdAsync)
      );

      return lastTxId.switchOnFirst((first, rest) ->
        tFlux.withLatestFrom(lastTxId, (record, published) -> {
          assert lag != null;
          lag.set(published - record.txid().value());
          return record;
        })
      );
    };

  }
}
