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
package org.robotninjas.stream.transfer;

import java.time.Duration;

import org.robotninjas.stream.Batching;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.proto.Replicate;
import org.robotninjas.stream.server.GetStart;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class StreamFactoryTransfer {
  private static final Batching<Record.Stored> BufferRecords =
    Batching.bySizeAndTime(10, Duration.ofMillis(10));

  private static final Batching<Offset> BufferOffsets =
    Batching.bySizeAndTime(10, Duration.ofMillis(10));

  private StreamFactoryTransfer() {}

  public static Transfer.Source source(StreamFactory factory) {
    return (Mono<Replicate.Request> requestMono) ->
      requestMono.flatMapMany(request ->
        factory.get(StreamName.of(request.getName()))
          .read(GetStart.fromProto(request.getStart()))
          .transform(BufferRecords)
          .map(ToTransfer.fn(request.getName()))
      );
  }

  public static Transfer.Sink sink(StreamFactory factory) {
    return (Flux<Replicate.Transfer> transferFlux) ->
      transferFlux.switchOnFirst((signal, xfers) -> {
        if (signal.hasValue()) {
          var xfer = signal.get();
          assert xfer != null;
          var mgr = factory.get(StreamName.of(xfer.getStream()));
          return mgr.write(xfers.map(ToRecords.apply()).flatMap(Flux::fromIterable))
            .transform(BufferOffsets).map(ToResponse.apply());
        } else if (signal.hasError()) {
          var throwable = signal.getThrowable();
          assert throwable != null;
          return Flux.error(throwable);
        }
        return Flux.error(IllegalStateException::new);
      });
  }
}
