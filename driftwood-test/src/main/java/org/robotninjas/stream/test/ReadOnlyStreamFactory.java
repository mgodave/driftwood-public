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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.commons.lang.NotImplementedException;
import org.reactivestreams.Publisher;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamManager;
import org.robotninjas.stream.StreamName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReadOnlyStreamFactory implements StreamFactory {
  private final Map<StreamName, Supplier<Publisher<Record.Stored>>> streams = new ConcurrentHashMap<>();

  private ReadOnlyStreamFactory(Map<StreamName, Supplier<Publisher<Record.Stored>>> streams) {
    this.streams.putAll(streams);
  }

  public static StreamFactory create(Map<StreamName, Supplier<Publisher<Record.Stored>>> streams) {
    return new ReadOnlyStreamFactory(streams);
  }

  public static StreamFactory create(StreamName stream, Supplier<Publisher<Record.Stored>> supplier) {
    return new ReadOnlyStreamFactory(Map.of(stream, supplier));
  }

  @Override
  public StreamManager get(StreamName name) {
    Supplier<Publisher<Record.Stored>> supplier;
    if ((supplier = streams.get(name)) != null) {
      return new ReadOnlyStreamManager(name, supplier);
    }
    throw new IllegalStateException("test stream not found");
  }

  @Override
  public void close() { }

  public static class ReadOnlyStreamManager implements StreamManager {
    private final StreamName name;
    private final Supplier<Publisher<Record.Stored>> testPublisherSupplier;

    public ReadOnlyStreamManager(StreamName name, Supplier<Publisher<Record.Stored>> testPublisherSupplier) {
      this.name = name;
      this.testPublisherSupplier = testPublisherSupplier;
    }

    @Override
    public StreamName stream() {
      return name;
    }

    @Override
    public Flux<Record.Stored> read(Start start) {
      return Flux.from(testPublisherSupplier.get());
    }

    @Override
    public Flux<Offset> write(Flux<Record> rf) {
      throw new NotImplementedException("Not implemented");
    }

    @Override
    public Mono<Boolean> truncate(Offset offset) {
      throw new NotImplementedException("Not implemented");
    }

    @Override
    public Mono<Long> markEnd() {
      throw new NotImplementedException("Not implemented");
    }

    @Override
    public void close() { }
  }
}
