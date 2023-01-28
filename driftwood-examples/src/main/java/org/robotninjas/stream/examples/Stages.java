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

import java.util.function.Function;

import reactor.core.publisher.Flux;

@SuppressWarnings("UnusedVariable")
public class Stages {

  interface Source<T> {
    Flux<T> read();
  }

  interface Stage<I, O> {
    Source<O> eval();
  }

  interface Serde<T> {
    byte[] serialize(T i);
    T deserialize(byte[] b);
  }

//  record InputStage<I>(StreamName output) implements Stage<I, I> {
//    @Override public Flux<I> eval(Flux<I> in) {
//      return in;
//    }
//  }

  record MapStage<I, O>(Source<I> source, Function<I, O> fn) implements Stage<I, O> {
    @Override public Source<O> eval() {
      return () -> source.read().map(fn);
    }
  }

//  record FilterStage<I>(Stage<?, I> in, Predicate<I> pred) implements Stage<I, I> {
//    @Override public Flux<I> eval(Flux<I> in) {
//      return in.filter(pred);
//    }
//
//    @Override public StreamName output() {
//      return null;
//    }
//  }
//
//  static <I> List<Stage<I, I>> branch(Stage<?, I> in, Predicate<I>... preds) {
//    return List.of(preds).stream()
//      .map(p -> new FilterStage<>(in, p))
//      .collect(Collectors.toList());
//  }
//
//  record TransferStage<I>(StreamName input, StreamName output) implements Stage<I, I> {
//    @Override public Flux<I> eval(Flux<I> in) {
//      return in;
//    }
//  }
//
//  static <I> List<Stage<I, I>> collect(StreamName out, Stage<?, I>... in) {
//    return List.of(in).stream()
//      .map(s -> new TransferStage<I>(s.output(), out))
//      .collect(Collectors.toList());
//  }

}
