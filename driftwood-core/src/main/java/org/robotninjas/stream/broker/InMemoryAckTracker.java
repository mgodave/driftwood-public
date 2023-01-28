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

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.SeqId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

class InMemoryAckTracker implements AckTracker {

  private final RangeSet<SeqId> acks = TreeRangeSet.create();
  private final Sinks.Many<Record.Stored> retries = Sinks.many()
    .multicast().onBackpressureBuffer();

  private final Duration delay;

  InMemoryAckTracker(Duration delay) {
    this.delay = delay;
  }

  @Override
  public void ack(List<SeqId> acks) {
    this.acks.add(Range.encloseAll(acks));
  }

  @Override
  public void send(List<Record.Stored> records) {
    records.forEach(retries::tryEmitNext);
  }

  @Override
  public Flux<Record.Stored> retries() {
    return retries.asFlux()
      .delayElements(delay)
      .filter(record -> acks.contains(record.seqId()));
  }
}
