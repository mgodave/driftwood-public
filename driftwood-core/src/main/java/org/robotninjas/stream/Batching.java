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

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public interface Batching<T> extends Function<Flux<T>, Publisher<List<T>>> {
  static <T> Batching<T> byTime(Duration duration) {
    return (Flux<T> input) -> input.buffer(duration);
  }

  @SuppressWarnings("PreferJavaTimeOverload")
  static <T> Batching<T> bySize(int maxSize) {
    return (Flux<T> input) -> input.buffer(maxSize);
  }

  static <T> Batching<T> bySizeAndTime(int maxSize, Duration duration) {
    return (Flux<T> input) -> input.bufferTimeout(maxSize, duration);
  }

  static <T> Batching<T> whileTrue(Predicate<T> predicate) {
    return (Flux<T> input) -> input.bufferWhile(predicate);
  }

  static <T> Batching<T> untilTrue(Predicate<T> predicate) {
    return (Flux<T> input) -> input.bufferUntil(predicate);
  }

  static <T> Batching<T> none() {
    return (Flux<T> input) -> input.map(List::of);
  }
}
