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

import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Adapt an {@link AsyncLogWriter} to a {@link Flux} based API
 */
@FunctionalInterface
interface StreamWriter {
  Flux<Offset> write(Flux<Record> rf);

  static StreamWriter create(AsyncLogWriter writer) {
    return (Flux<Record> rf) ->
      rf.map(record -> new LogRecord(record.txid().value(), record.data()))
        .flatMap(r -> Mono.fromCompletionStage(writer.write(r))).map(DLOffset::new);
  }
}
