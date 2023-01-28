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

@SuppressWarnings({"JavaLangClash", "UnusedVariable"})
public interface Record {
   static Record create(ByteBuffer data, TxId txid) {
      return new SimpleRecord(data, txid);
   }

   static Record create(byte[] data, TxId txid) {
      return new SimpleRecord(ByteBuffer.wrap(data).asReadOnlyBuffer(), txid);
   }

   static Stored stored(ByteBuffer data, TxId txid, Offset offset, SeqId seqId) {
      return new SimpleStoredRecord(data, txid, offset, seqId);
   }

   ByteBuffer data();

   TxId txid();

   interface Stored extends Record {
      Offset offset();

      SeqId seqId();
   }

   record SimpleRecord(ByteBuffer data, TxId txid) implements Record {}

   record SimpleStoredRecord(ByteBuffer data, TxId txid, Offset offset, SeqId seqId) implements Stored {}

   interface StoredAdapter extends Record.Stored {
      Record record();

      @Override
      default ByteBuffer data() {
         return record().data();
      }

      @Override
      default TxId txid() {
         return record().txid();
      }

      @Override
      default Offset offset() {
         return Offset.Empty;
      }

      @Override
      default SeqId seqId() {
         return SeqId.Empty;
      }
   }
}
