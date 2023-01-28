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
package org.robotninjas.stream;

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.robotninjas.stream.proto.Read;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

@FunctionalInterface
public
interface ToReadRecord extends Function<List<Record.Stored>, Read.Response> {
  static Read.Record toReadRecord(Record.Stored record) {
    return Read.Record.newBuilder()
      .setData(unsafeWrap(record.data()))
      .setOffset(unsafeWrap(record.offset().bytes()))
      .setTxid(record.txid().value())
      .setSeq(record.seqId().value())
      .build();
  }

  static List<Read.Record> batched(List<Record.Stored> storedRecords) {
    ImmutableList.Builder<Read.Record> readRecords =
      ImmutableList.builderWithExpectedSize(storedRecords.size());
    for (Record.Stored storedRecord : storedRecords) {
      readRecords.add(toReadRecord(storedRecord));
    }
    return readRecords.build();
  }
}
