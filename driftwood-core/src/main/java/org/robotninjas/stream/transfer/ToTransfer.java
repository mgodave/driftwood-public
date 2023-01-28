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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.checkerframework.dataflow.qual.Pure;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.proto.Replicate;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

@FunctionalInterface
interface ToTransfer extends Function<List<? extends Record>, Replicate.Transfer> {
  static List<Replicate.Record> toRecords(List<? extends Record> records) {
    List<Replicate.Record> output = new ArrayList<>(records.size());
    for (var record : records) {
      output.add(Replicate.Record.newBuilder()
        .setTxid(record.txid().value())
        .setData(unsafeWrap(record.data()))
        .build());
    }
    return output;
  }

  @Pure static ToTransfer fn(String streamName) {
    return (List<? extends Record> records) ->
      Replicate.Transfer.newBuilder()
        .setStream(streamName)
        .addAllData(toRecords(records))
        .build();
  }
}
