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

import java.util.List;
import java.util.function.Function;

import org.robotninjas.stream.Record;
import org.robotninjas.stream.SeqId;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public interface AckTracker {
  default void ack(SeqId seqId) {
    ack(List.of(seqId));
  }

  void ack(List<SeqId> acks);

  default void send(Record.Stored record) {
    send(List.of(record));
  }

  void send(List<Record.Stored> records);

  Flux<Record.Stored> retries();

  default Scheduler scheduler() {
    return Schedulers.immediate();
  }

  default Function<Flux<Record.Stored>, Flux<Record.Stored>> track() {
    return results -> results.doOnNext(records ->
      scheduler().schedule(() -> send(records))
    );
  }

  default Function<Flux<SeqId>, Flux<SeqId>> acknowledge() {
    return results -> results.doOnNext(offsets ->
      scheduler().schedule(() -> ack(offsets))
    );
  }

  AckTracker NullTracker = new AckTracker() {
    @Override
    public void ack(List<SeqId> acks) {}

    @Override
    public void send(List<Record.Stored> records) {}

    @Override
    public Flux<Record.Stored> retries() {
      return Flux.empty();
    }
  };
}
