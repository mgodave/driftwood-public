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
package org.robotninjas.stream.examples;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.robotninjas.stream.StreamName;

import static java.time.Duration.ofSeconds;

@SuppressWarnings("UnusedVariable")
public class FunctionsBuilder {
  public static <I> AndThen<I> read(StreamName streamName, Serde<I> serde) {
    return null;
  }

  public static void main(String[] args) {

    final var SerdeIn = new IntegerSerde();
    final var SerdeOut = new IntegerSerde();

    FunctionsBuilder
      .read(StreamName.of("start"), SerdeIn)
      .andThen(Map.apply(in -> in), SerdeOut)
      .andThen(GroupBy.apply(in -> 1L), SerdeOut)
      .andThen(Map.apply(in -> in), SerdeOut)
      .andThen(Window.apply(ofSeconds(2)), SerdeOut)
      .andThen(Map.apply(in -> in), SerdeOut)
      .andThen(Filter.apply(in -> true), SerdeOut)
      .andThen(Collect.apply(), SerdeOut)
      .andThen(WithEffect.apply(in -> {}), SerdeOut);

    FunctionsBuilder
      .read(StreamName.of("start"), SerdeIn)
      .map(in -> in, SerdeOut)
      .groupBy(in -> 1L, SerdeOut)
      .map(in -> in, SerdeOut)
      .window(ofSeconds(2), SerdeOut)
      .map(in -> in, SerdeOut)
      .filter(in -> true, SerdeOut)
      .collect(SerdeOut)
      .withEffect(in -> {}, SerdeOut);

  }

  static class IntegerSerde implements Serde<Integer> {
    @Override
    public byte[] serialize(Integer type) {
      return new byte[0];
    }

    @Override
    public Integer deserialize(byte[] bytes) {
      return null;
    }
  }

  interface Serde<T> {
    byte[] serialize(T type);
    T deserialize(byte[] bytes);
  }

  interface Step<I, O> extends Function<I, O> {
    List<StreamName> output();
    Serde<O> serde();
  }

  interface Map<I, O> extends Step<I, O> {
    static <I, O> Map<I, O> apply(Function<I, O> mapF) {
      return null;
    }
  }

  interface GroupBy<I> extends Step<I, I> {
    static <I> GroupBy<I> apply(Function<I, Long> partF) {
      return null;
    }
  }

  interface Window<I> extends Step<I, I> {
    static <I> Window<I> apply(Duration time) {
      return null;
    }
  }

  interface WithEffect<I> extends Step<I, I> {
    static <I> WithEffect<I> apply(Consumer<I> consumer) {
      return null;
    }
  }

  interface Collect<I> extends Step<I, I> {
    static <I> Collect<I> apply() {
      return null;
    }
  }

  interface Filter<I> extends Step<I, I> {
    static <I> Filter<I> apply(Function<I, Boolean> filterF) {
      return null;
    }
  }

  interface AndThen<I> {
    <O> AndThen<O> andThen(Step<I, O> f, Serde<O> serde);
    <O> AndThen<O> map(Function<I, O> mapF, Serde<O> serde);
    AndThen<I> groupBy(Function<I, Long> groupF, Serde<I> serde);
    AndThen<I> window(Duration duration, Serde<I> serde);
    AndThen<I> withEffect(Consumer<I> consumer, Serde<I> serde);
    AndThen<I> collect(Serde<I> serde);
    AndThen<I> filter(Function<I, Boolean> filterF, Serde<I> serde);
  }

  interface Begin<I> {
    AndThen<I> read(StreamName streamName);
  }
}
