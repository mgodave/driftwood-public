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
package org.robotninjas.stream.grpc;

import com.be_hase.grpc.micrometer.MicrometerClientInterceptor;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.grpc.ManagedChannel;
import io.micrometer.core.instrument.Metrics;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.client.StreamWriter;
import org.robotninjas.stream.proto.ReactorWriteProxyGrpc;
import org.robotninjas.stream.proto.ReactorWriteProxyGrpc.ReactorWriteProxyStub;
import org.robotninjas.stream.security.AuthToken;
import reactor.core.publisher.Flux;

@SuppressWarnings("UnstableApiUsage")
public class GrpcStreamWriter extends AbstractExecutionThreadService {
  private final ManagedChannel channel;
  private final String streamName;
  private final Flux<Record> records;
  private final AuthToken authToken;

  public GrpcStreamWriter(
    ManagedChannel channel,
    String streamName,
    Flux<Record> records,
    AuthToken authToken
  ) {
    this.channel = channel;
    this.streamName = streamName;
    this.records = records;
    this.authToken = authToken;
  }

  @Override
  protected void run() {
    ReactorWriteProxyStub stub = ReactorWriteProxyGrpc.newReactorStub(channel)
      .withCallCredentials(new AuthTokenCallCredentials(authToken))
      .withInterceptors(new MicrometerClientInterceptor(Metrics.globalRegistry));

    StreamWriter.from(stub::write)
      .write(streamName, records)
      .doOnNext(ignore -> System.out.print("."))
      .then().block();
  }
}
