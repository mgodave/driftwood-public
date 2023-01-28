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
package org.robotninjas.app.dl;

import java.util.Arrays;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.namespace.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("UnstableApiUsage")
public class DLWriter extends AbstractExecutionThreadService {
  private static final Logger Log = LoggerFactory.getLogger(DLWriter.class);

  private final Namespace dlns;
  private final String streamName;
  private final int writeCount;
  private final int packetSize;

  public DLWriter(Namespace dlns, String streamName, int writeCount, int packetSize) {
    this.dlns = dlns;
    this.streamName = streamName;
    this.writeCount = writeCount;
    this.packetSize = packetSize;
  }

  @Override
  protected void run() {
    var bytes = new byte[packetSize];
    Arrays.fill(bytes, (byte) 10);

    Flux.usingWhen(
      Mono.fromCallable(() -> dlns.openLog(streamName)),
      mgr -> Flux.usingWhen(
        Mono.fromCompletionStage(mgr.openAsyncLogWriter()),
        writer -> Flux.range(0, writeCount).flatMapSequential(i ->
          Mono.fromCompletionStage(
            writer.write(new LogRecord(i, bytes))
          )
        ),
        writer -> Mono.empty()//Mono.fromCompletionStage(writer.asyncClose())
      ),
      mgr -> Mono.fromCompletionStage(mgr::asyncClose)
    ).then().block();

    Log.info("DONE");
  }
}
