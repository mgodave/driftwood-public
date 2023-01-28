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

import java.util.List;

import org.robotninjas.stream.Record;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface BatchProcessor {
  Mono<Void> process(List<? extends Record> batch);

  BatchProcessor Noop = ignore -> Mono.empty();

  BatchProcessor Dots = ignore -> {
    System.out.print(".");
    return Mono.empty();
  };
}
