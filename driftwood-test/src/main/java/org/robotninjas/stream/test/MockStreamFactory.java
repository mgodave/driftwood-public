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
package org.robotninjas.stream.test;

import org.robotninjas.stream.*;
import org.robotninjas.stream.Record;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MockStreamFactory implements StreamFactory {
  private final Flux<Record.Stored> records;

  public MockStreamFactory(Flux<Record.Stored> records) {
    this.records = records;
  }

  @Override
  public StreamManager get(StreamName streamName) {
    return new StreamManager() {
      @Override
      public StreamName stream() {
        return streamName;
      }
      
      @Override
      public Flux<Record.Stored> read(Start start) {
        return records;
      }

      @Override
      public Flux<Offset> write(Flux<Record> rf) {
        return rf.map(record ->
          new LongOffset(record.txid().value())
        );
      }

      @Override
      public Mono<Boolean> truncate(Offset offset) {
        return Mono.just(true);
      }

      @Override
      public Mono<Long> markEnd() {
        return Mono.just(-1L);
      }

      @Override
      public void close() {

      }
    };
  }

  @Override
  public void close() {
  }

}
