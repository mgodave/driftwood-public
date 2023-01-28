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

import org.junit.Test;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.TestUtils;
import org.robotninjas.stream.proto.Replicate;
import org.robotninjas.stream.proto.Start;
import org.robotninjas.stream.test.MockStreamFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.robotninjas.stream.transfer.StreamFactoryTransfer.sink;
import static org.robotninjas.stream.transfer.StreamFactoryTransfer.source;

public class RawTransferTest {
  private static final Replicate.Request REQUEST =
    Replicate.Request.newBuilder()
      .setName("StreamName")
      .setStart(Start.newBuilder()
        .setFromNewest(
          Start.FromNewest.newBuilder()
        )
      ).build();

  @Test
  public void testLocalTransfer() {
    var testRecords = TestUtils.generateRecords(10);
    StreamFactory streamFactory = new MockStreamFactory(
      Flux.fromIterable(testRecords)
    );

    var xfer = new Transfer.RawTransfer(
      source(streamFactory),
      sink(streamFactory)
    );

    StepVerifier.create(xfer.apply(REQUEST))
      .expectNextCount(1);

  }
}