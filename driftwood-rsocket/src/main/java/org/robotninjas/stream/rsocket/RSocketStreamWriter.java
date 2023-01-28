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
package org.robotninjas.stream.rsocket;

import java.nio.charset.StandardCharsets;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.micrometer.core.instrument.Metrics;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.AuthMetadataCodec;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.WellKnownAuthType;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.micrometer.MicrometerRSocketInterceptor;
import io.rsocket.transport.ClientTransport;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.client.StreamWriter;
import org.robotninjas.stream.proto.WriteProxyClient;
import org.robotninjas.stream.security.AuthToken;
import reactor.core.publisher.Flux;

import static io.rsocket.frame.decoder.PayloadDecoder.ZERO_COPY;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("UnstableApiUsage")
public class RSocketStreamWriter extends AbstractExecutionThreadService {
  private final ClientTransport transport;
  private final String streamName;
  private final Flux<Record> records;
  private final AuthToken token;

  public RSocketStreamWriter(
    ClientTransport transport,
    String streamName,
    Flux<Record> records,
    AuthToken token
  ) {
    this.transport = transport;
    this.streamName = streamName;
    this.records = records;
    this.token = token;
  }

  @Override
  protected void run() {
    RSocketConnector.create()
      .payloadDecoder(ZERO_COPY)
      .resume(new Resume())
      .interceptors(registry ->
        registry.forRequester(new MicrometerRSocketInterceptor(Metrics.globalRegistry))
      ).connect(transport).flatMapMany(socket -> {
        var client = new WriteProxyClient(socket, Metrics.globalRegistry);
        return StreamWriter.from(request ->
          client.write(request, Metadata.make(token))
        ).write(streamName, records);
      }
    ).then().block();
  }
}
