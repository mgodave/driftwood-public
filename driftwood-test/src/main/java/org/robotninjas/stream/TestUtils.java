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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.robotninjas.stream.Record.SimpleStoredRecord;

import static java.util.stream.Collectors.toList;

public class TestUtils {
  private static final Random rand = new Random();

  private static Record.Stored newRecord(byte[] bytes, Offset offset, TxId txid) {
    return new SimpleStoredRecord(ByteBuffer.wrap(bytes), txid, offset, SeqId.Empty);
  }

  private static byte[] generateData() {
    var bytes = new byte[10];
    rand.nextBytes(bytes);
    return bytes;
  }

  public static List<Record.Stored> generateRecords(int n) {
    return IntStream.range(0, n).mapToObj(i ->
      newRecord(generateData(), new LongOffset(i), new TxId(n))
    ).toList();
  }

  public static Stream<Record.Stored> generateRecordsStream(int n) {
    return IntStream.range(0, n).mapToObj(i ->
      newRecord(generateData(), new LongOffset(i), new TxId(n))
    );
  }
}
