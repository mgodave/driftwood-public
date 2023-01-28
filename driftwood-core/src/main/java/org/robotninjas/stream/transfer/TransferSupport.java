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
import java.util.Objects;

import org.robotninjas.stream.Batching;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.filters.MetricsFilter;
import org.robotninjas.stream.filters.MetricsFilter.Tags;
import org.robotninjas.stream.proto.Replicate;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Permissions;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.server.GetStart;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

final public class TransferSupport {
  private static final Batching<Record.Stored> BufferRecordsBySizeAndTime =
    Batching.bySizeAndTime(10, Duration.ofMillis(10));

  private static final Batching<Offset> BufferOffsetsBySizeAndTime =
    Batching.bySizeAndTime(10, Duration.ofMillis(10));

  private final StreamFactory streamFactory;
  private final Security security;

  public TransferSupport(StreamFactory streamFactory, Security security) {
    this.streamFactory = streamFactory;
    this.security = security;
  }

  public Flux<Replicate.Transfer> read(Mono<Replicate.Request> request, CallContext ctx) {
    return security.login(ctx).flatMapMany(principal ->
      request.flatMapMany(req -> {
        var stream = Objects.requireNonNullElse(
          StreamName.of(req.getName()),
          StreamName.Empty
        );

        return principal.check(Permissions.pull(stream)).flatMapMany(ignore ->
          streamFactory.get(stream)
            .read(GetStart.fromProto(req.getStart()))
            .transform(Security.op(principal, Permissions.pull(stream)))
            .transform(MetricsFilter.op(Tags.pull(stream)))
            .transform(BufferRecordsBySizeAndTime)
            .map(ToTransfer.fn(req.getName()))
        );
      }));
  }

  public Flux<Replicate.Response> write(Flux<Replicate.Transfer> transfer, CallContext ctx) {
    return security.login(ctx).flatMapMany(principal ->
      transfer.switchOnFirst((signal, writeRequests) -> {
        if (signal.hasValue()) {
          var xfer = requireNonNull(signal.get());
          var stream = StreamName.of(xfer.getStream());

          var requests = writeRequests
            .transform(Security.op(principal, Permissions.push(stream)))
            .map(ToRecords.apply())
            .flatMap(Flux::fromIterable);

          return streamFactory.get(stream)
            .write(requests)
            .transform(MetricsFilter.op(Tags.push(stream)))
            .transform(BufferOffsetsBySizeAndTime)
            .map(ToResponse.apply());

        } else if (signal.hasError()) {
          var throwable = signal.getThrowable();
          assert throwable != null;
          return Flux.error(throwable);
        }
        return Flux.error(IllegalStateException::new);
      }));
  }
}
