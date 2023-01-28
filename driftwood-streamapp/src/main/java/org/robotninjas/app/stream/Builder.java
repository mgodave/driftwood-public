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
package org.robotninjas.app.stream;

import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.function.Function.identity;

/**
 * A simple wrapper around composition.
 */
@SuppressWarnings("UnusedReturnValue")
public class Builder<T> {

  private Function<T, T> steps = identity();

  public synchronized Builder<T> add(Function<T, T> step) {
    steps = steps.andThen(step);
    return this;
  }

  public synchronized T build(Supplier<T> init) {
    return steps.apply(init.get());
  }

  public synchronized T build(T init) {
    return steps.apply(init);
  }

  public static <T> Builder<T> newBuilder() {
    return new Builder<>();
  }
}
