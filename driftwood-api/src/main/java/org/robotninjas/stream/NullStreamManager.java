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
package org.robotninjas.stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

final class NullStreamManager implements StreamManager {
   @Override
   public StreamName stream() {
      return null;
   }

   @Override
   public Flux<Record.Stored> read(Start start) {
      return Flux.empty();
   }

   @Override
   public Flux<Offset> write(Flux<Record> rf) {
      return Flux.empty();
   }

   @Override
   public Mono<Boolean> truncate(Offset offset) {
      return Mono.empty();
   }

   @Override
   public Mono<Long> markEnd() {
      return Mono.empty();
   }

   @Override
   public void close() {

   }
}
