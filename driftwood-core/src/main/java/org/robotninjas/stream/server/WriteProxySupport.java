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
package org.robotninjas.stream.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.reactivestreams.Publisher;
import org.robotninjas.stream.Batching;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.TxId;
import org.robotninjas.stream.filters.MetricsFilter;
import org.robotninjas.stream.filters.MetricsFilter.Tags;
import org.robotninjas.stream.proto.MarkEnd;
import org.robotninjas.stream.proto.Truncate;
import org.robotninjas.stream.proto.Write;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Permissions;
import org.robotninjas.stream.security.Security;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.Objects.requireNonNull;

final public class WriteProxySupport {
  private static final Batching<Offset> BufferOffsetsBySizeAndTime =
    Batching.bySizeAndTime(10, Duration.ofMillis(10));

  private final StreamFactory streamFactory;
  private final Security security;

  public WriteProxySupport(StreamFactory streamFactory, Security security) {
    this.streamFactory = streamFactory;
    this.security = security;
  }

  public Flux<Write.Response> write(Flux<Write.Request> requestFlux, CallContext ctx) {
    return security.login(ctx).flatMapMany(principal ->
      requestFlux.switchOnFirst((signal, writeRequests) -> {
        if (signal.hasValue()) {
          Write.Request request = signal.get();
          assert request != null;

          var stream = StreamName.of(requireNonNull(request.getStream()));

          var requests = writeRequests
            .map(ToRecords.apply())
            .flatMap(Flux::fromIterable)
            .transform(Security.op(principal, Permissions.write(stream)));

          return streamFactory.get(stream)
            .write(requests)
            .transform(BufferOffsetsBySizeAndTime)
            .map(ToWriteResponse.apply())
            .transform(MetricsFilter.op(Tags.write(stream)))
            .transform(WithContext.apply(stream));

        }

        return Flux.error(IllegalStateException::new);
      }));
  }

  public Mono<Truncate.Response> truncate(Mono<Truncate.Request> request, CallContext ctx) {
    return security.login(ctx).flatMap(principal ->
      request.flatMap(truncate -> {
        var stream = StreamName.of(requireNonNull(truncate.getStream()));

        Function<Boolean, Truncate.Response> resultToResponse = (status) ->
          Truncate.Response.newBuilder().setSuccess(
            Truncate.Success.newBuilder().setSuccess(status)
          ).build();

        var offset = Offset.from(truncate.getOffset().toByteArray());
        return principal.check(Permissions.truncate(stream)).flatMap(ignore ->
          streamFactory.get(stream).truncate(offset)
            .map(resultToResponse)
            .transform(MetricsFilter.op(Tags.truncate(stream)))
            .transform(WithContext.apply(stream))
        );
      }));
  }

  public Mono<MarkEnd.Response> markEnd(Mono<MarkEnd.Request> request, CallContext ctx) {
    return security.login(ctx).flatMap(principal -> request.flatMap(mark -> {
      var stream = StreamName.of(requireNonNull(mark.getStream()));

      Function<Long, MarkEnd.Response> resultToResponse = txid ->
        MarkEnd.Response.newBuilder().setSuccess(
          MarkEnd.Success.newBuilder().setTxid(txid)
        ).build();

      return principal.check(Permissions.end(stream)).flatMap(ignore ->
        streamFactory.get(stream).markEnd()
          .transform(Security.op(principal, Permissions.end(stream)))
          .map(resultToResponse)
          .transform(MetricsFilter.op(Tags.markEnd(stream)))
          .transform(WithContext.apply(stream))
      );
    }));
  }

  @FunctionalInterface
  interface ToWriteResponse extends Function<List<? extends Offset>, Write.Response> {
    static ToWriteResponse apply() {
      return (List<? extends Offset> offsets) -> {
        var success = Write.Success.newBuilder();
        success.addAllOffset(offsets.stream().map(
          offset -> unsafeWrap(offset.bytes())).toList()
        );
        return Write.Response.newBuilder().setSuccess(success).build();
      };
    }
  }

  @FunctionalInterface
  interface ToRecords extends Function<Write.Request, Collection<Record>> {
    static ToRecords apply() {
      return (Write.Request request) ->
        request.getDataList().stream().map(data ->
          Record.create(data.getData().asReadOnlyByteBuffer(), new TxId(data.getTxid()))
        ).toList();
    }
  }

  interface WithContext {
    static <T> Function<Publisher<T>, Publisher<T>> apply(StreamName stream) {
      return response -> Flux.from(response).contextWrite(ctx ->
        ctx.putNonNull(ContextKeys.StreamName, stream.name())
      );
    }
  }
}
