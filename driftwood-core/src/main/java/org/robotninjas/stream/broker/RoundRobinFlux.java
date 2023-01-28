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

import com.google.common.collect.Iterators;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

public class RoundRobinFlux<T> extends ConnectableFlux<T> {
  private final Flux<T> source;
  private final ConcurrentLinkedDeque<Sinks.Many<T>> sinks = new ConcurrentLinkedDeque<>();

  private RoundRobinFlux(Flux<T> source) {
    this.source = source;
  }

  @Override
  public void connect(Consumer<? super Disposable> cancelSupport) {
    var itr = Iterators.cycle(sinks);
    var subscriber = new BaseSubscriber<T>() {
      @Override
      protected void hookOnNext(T value) {
        loop:
        for (; ; ) {
          Sinks.Many<T> sink = itr.next();
          switch (sink.tryEmitNext(value)) {
            case FAIL_CANCELLED:
            case FAIL_TERMINATED:
            case FAIL_ZERO_SUBSCRIBER:
              itr.remove();
            case FAIL_OVERFLOW:
              // try another one
              break;
            case OK:
              break loop;
            default:
          }
        }
      }
    };

    source.subscribe(subscriber);
    subscriber.requestUnbounded();
    cancelSupport.accept(subscriber);
  }

  @Override
  public void subscribe(CoreSubscriber<? super T> actual) {
    Sinks.Many<T> sink = Sinks.many().unicast().onBackpressureBuffer();
    sink.asFlux().subscribe(actual);
    sinks.add(sink);
  }

  public static <T> ConnectableFlux<T> make(Flux<T> source) {
    return new RoundRobinFlux<>(source);
  }
}
