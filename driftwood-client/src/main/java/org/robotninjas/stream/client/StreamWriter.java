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
package org.robotninjas.stream.client;

import java.util.List;
import java.util.function.Function;

import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.proto.Write;
import reactor.core.publisher.Flux;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

final public class StreamWriter {
  private static final int NumBuffered = 10;

  private final Function<Flux<Write.Request>, Flux<Write.Response>> f;

  private StreamWriter(Function<Flux<Write.Request>, Flux<Write.Response>> f) {
    this.f = f;
  }

  private static Write.Request toRequest(String stream, List<? extends Record> batch) {
    var writeRequest = Write.Request.newBuilder().setStream(stream);
    for (Record record : batch) {
      Write.Record logRecord = Write.Record.newBuilder()
        .setTxid(record.txid().value())
        .setData(unsafeWrap(record.data()))
        .build();
      writeRequest.addData(logRecord);
    }
    return writeRequest.build();
  }

  private static Flux<? extends Offset> getOffsets(Write.Response response) {
    return switch (response.getResponseCase()) {
      case SUCCESS -> {
        var success = response.getSuccess();
        yield Flux.fromIterable(success.getOffsetList()).map(offset ->
          Offset.from(offset.toByteArray())
        );
      }
      case FAILURE -> Flux.error(new Exception("unknown error"));
      case RESPONSE_NOT_SET -> Flux.error(
        new IllegalStateException("neither success nor failure specified in response")
      );
    };
  }

  @SuppressWarnings({"PreferJavaTimeOverload", "UnstableApiUsage"})
  public Flux<? extends Offset> write(String stream, Flux<? extends Record> records) {
    var buffered = records
      .buffer(NumBuffered)
      .map(batch -> toRequest(stream, batch));

    return f.apply(buffered).flatMap(StreamWriter::getOffsets);
  }

  public static StreamWriter from(Function<Flux<Write.Request>, Flux<Write.Response>> f) {
    return new StreamWriter(f);
  }
}
