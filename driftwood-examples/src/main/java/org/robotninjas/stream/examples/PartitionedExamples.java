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

import java.util.List;
import java.util.function.Function;

import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static reactor.core.publisher.Flux.fromStream;
import static reactor.core.publisher.Flux.merge;

@SuppressWarnings("UnusedVariable")
public class PartitionedExamples {
  static Function<Flux<List<PartitionAndStart>>, Flux<PartitionedRecord>> partitioned(StreamFactory streamFactory) {
    return (Flux<List<PartitionAndStart>> partitioning) ->
      partitioning.switchMap(partitions ->
        merge(fromStream(
          partitions.stream().map(p ->
            Flux.usingWhen(
              Mono.just(streamFactory.get(p.partition)),
              stream -> stream.read(p.from),
              stream -> Mono.<Void>fromCallable(() -> {
                stream.close();
                return null;
              })
            ).map(r -> new PartitionedRecord(r, p.partition))
          ))
        )
      );
  }

  public static void main(String[] args) {
    StreamFactory streamFactory = null;
    Flux.usingWhen(
      Mono.just(streamFactory),
      factory -> Mono.empty(),
      factory -> Mono.<Void>fromCallable(() -> {
        factory.close();
        return null;
      })
    ).publish().refCount();

//    var cacheMap = new ConcurrentHashMap<String, Signal<? extends StreamFactory.StreamManager>>();
//    CacheMono.lookup(key -> Mono.just(cacheMap.get(key)), "key")
//      .onCacheMissResume(() -> Mono.just(streamFactory.get(named("key"))))
//      .andWriteWith((key, signal) -> {
//        cacheMap.put(key, signal);
//        return Mono.empty();
//      });
  }

  record PartitionName(String name, long partition) implements StreamName { }
  record PartitionAndStart(PartitionName partition, Start from) { }
  record PartitionedRecord(Record.Stored record, PartitionName partition) { }
}
