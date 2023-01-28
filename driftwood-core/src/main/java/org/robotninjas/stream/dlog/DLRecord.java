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

import java.nio.ByteBuffer;

import org.apache.distributedlog.LogRecordWithDLSN;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.SeqId;
import org.robotninjas.stream.TxId;

public record DLRecord(ByteBuffer data, DLOffset offset, SeqId seqId, TxId txid) implements Record.Stored {
  public DLRecord(LogRecordWithDLSN logRecordWithDLSN) {
    this(
      logRecordWithDLSN.getPayloadBuf().nioBuffer().asReadOnlyBuffer(),
      new DLOffset(logRecordWithDLSN.getDlsn()),
      new SeqId(logRecordWithDLSN.getSequenceId()),
      new TxId(logRecordWithDLSN.getTransactionId())
    );
  }
}

