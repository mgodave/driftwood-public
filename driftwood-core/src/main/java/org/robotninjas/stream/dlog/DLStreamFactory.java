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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.robotninjas.stream.*;
import org.robotninjas.stream.Record;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

final public class DLStreamFactory implements StreamFactory {

  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final ManagerCache managerCache;
  private final WriterCache writerCache;

  public DLStreamFactory(Namespace dlns) {
    this.managerCache = new ManagerCache(dlns);
    this.writerCache = new WriterCache(managerCache);
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      writerCache.close();
      managerCache.close();
    }
  }

  @Override
  public StreamManager get(StreamName streamName) {
    return new DLStreamManager(streamName);
  }

  private class DLStreamManager implements StreamManager {
    private final StreamName streamName;

    DLStreamManager(StreamName name) {
      this.streamName = name;
    }

    @Override
    public StreamName stream() {
      return streamName;
    }

    private Mono<DLSN> getOffset(Start start, DistributedLogManager mgr) {
      return switch (start) {
        case Start.FromNewest ignore ->
          Mono.fromCompletionStage(mgr.getLastDLSNAsync());
        case Start.FromOldest ignore -> Mono.just(DLSN.InitialDLSN);
        case Start.FromOffset fromOffset ->
          Mono.just(DLSN.deserializeBytes(fromOffset.offset().bytes()));
      };
    }

    @Override
    public Flux<Record.Stored> read(Start start) {
      // usingWhen allows us to grab a reference from the cache,
      // which uses weakRefs, and hold on to it until we're done.
      // At the end, we simply allow the reference to expire and
      // let the cache close the manager if necessary.
      return managerCache.withManager(streamName.name(), mgr ->
        getOffset(start, mgr).flatMapMany(dlsn ->
          StreamReader.create(mgr).read(dlsn)
            .subscribeOn(Schedulers.boundedElastic(), false)
            //TODO these don'cause belong at this scope, they belong much higher.
            //.transform(MessageSentRecorder.operator())
            //.transform(LagRecorder.operator(mgr))
        )
      );
    }

    public Flux<Record.Stored> read(DLOffset start, DLOffset end) {
      return read(new Start.FromOffset(start)).takeWhile(record -> {
        var offset = (DLOffset) record.offset();
        return offset.compareTo(end) < 0;
      });
    }

    @Override
    public Flux<Offset> write(Flux<Record> rf) {
      return writerCache.withWriter(streamName.name(), writer ->
        StreamWriter.create(writer).write(rf)
      );
    }

    @Override
    public Mono<Boolean> truncate(Offset offset) {
      return Mono.fromDirect(
        writerCache.withWriter(streamName.name(), writer ->
          Mono.fromCompletionStage(() ->
            writer.truncate(DLSN.deserializeBytes(offset.bytes()))
          )
        )
      );
    }

    @Override
    public Mono<Long> markEnd() {
      return Mono.fromDirect(
        writerCache.withWriter(streamName.name(), writer ->
          Mono.fromCompletionStage(
            writer::markEndOfStream
          )
        )
      );
    }

    @Override
    public void close() {

    }
  }

}
