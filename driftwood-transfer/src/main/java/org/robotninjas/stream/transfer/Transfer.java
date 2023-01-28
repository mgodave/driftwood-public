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

import java.util.List;

import com.google.protobuf.ByteString;
import org.checkerframework.dataflow.qual.Pure;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.proto.Replicate;
import org.robotninjas.stream.proto.Start.FromNewest;
import org.robotninjas.stream.proto.Start.FromOffset;
import org.robotninjas.stream.proto.Start.FromOldest;
import org.robotninjas.stream.transform.Transform;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.stream.Collectors.toUnmodifiableList;

public class Transfer {
  private final RawTransfer xfer;

  private Transfer(RawTransfer xfer) {
    this.xfer = xfer;
  }

  private org.robotninjas.stream.proto.Start getStart(Start start) {
    var builder = org.robotninjas.stream.proto.Start.newBuilder();
    return switch (start) {
      case Start.FromNewest ignore -> builder.setFromNewest(FromNewest.newBuilder()).build();
      case Start.FromOldest ignore -> builder.setFromOldest(FromOldest.newBuilder()).build();
      case Start.FromOffset fromOffset -> builder.setFromOffset(FromOffset.newBuilder()
        .setOffset(unsafeWrap(fromOffset.offset().bytes()))).build();
    };
  }

  private Mono<Void> doStart(StreamName from, Start start, Transform<Replicate.Transfer, Replicate.Transfer> xform) {
    var request = Replicate.Request.newBuilder()
      .setName(from.name())
      .setStart(getStart(start))
      .build();

    return xfer.transform(xform).apply(request).then();
  }

  public Mono<Void> start(StreamName stream, Start start) {
    return doStart(stream, start, Transform.identity());
  }

  public Mono<Void> start(StreamName from, Start start, StreamName to) {
    return doStart(from, start, xfer ->
      xfer.toBuilder().setStream(to.name()).build()
    );
  }

  public static Transfer create(Source source, Sink sink) {
    return new Transfer(new RawTransfer(source, sink));
  }

  @Pure
  private static List<Replicate.Record> doTransform(Transform<ByteString, ByteString> xform, Replicate.Transfer xfer) {
    return xfer.getDataList().stream().map(record ->
      record.toBuilder().setData(
        xform.apply(record.getData())
      ).build()
    ).toList();
  }

  @Pure
  static Transfer.Sink sink(Transfer.Sink sink, Transform<ByteString, ByteString> xform) {
    return (Flux<Replicate.Transfer> xfers) ->
      sink.write(xfers.map(xfer ->
        xfer.toBuilder().addAllData(
          doTransform(xform, xfer)
        ).build()
      ));
  }

  @Pure
  static Transfer.Source source(Transfer.Source source, Transform<ByteString, ByteString> xform) {
    return (Mono<Replicate.Request> request) ->
      source.read(request).map(xfer ->
        xfer.toBuilder().addAllData(
          doTransform(xform, xfer)
        ).build()
      );
  }

  @Pure
  static Transfer.Sink raw(Transfer.Sink sink, Transform<Replicate.Transfer, Replicate.Transfer> xform) {
    return (Flux<Replicate.Transfer> xfers) -> sink.write(xfers.map(xform));
  }

  @Pure
  static Transfer.Source raw(Transfer.Source source, Transform<Replicate.Transfer, Replicate.Transfer> xform) {
    return (Mono<Replicate.Request> request) -> source.read(request).map(xform);
  }

  @FunctionalInterface
  public interface Source {
    Flux<Replicate.Transfer> read(Mono<Replicate.Request> requests);

    default Source transform(Transform<Replicate.Transfer, Replicate.Transfer> xform) {
      return Transfer.raw(this, xform);
    }

    Source Empty = ignore -> Flux.empty();
  }

  @FunctionalInterface
  public interface Sink {
    Flux<Replicate.Response> write(Flux<Replicate.Transfer> transfers);

    default Sink transform(Transform<Replicate.Transfer, Replicate.Transfer> xform) {
      return Transfer.raw(this, xform);
    }
  }

  record RawTransfer(Source source, Sink sink) {
    public Flux<Replicate.Response> apply(Replicate.Request request) {
      return sink.write(source.read(Mono.just(request)));
    }

    public RawTransfer transform(Transform<Replicate.Transfer, Replicate.Transfer> xform) {
      return new RawTransfer(source.transform(xform), sink);
    }
  }
}
