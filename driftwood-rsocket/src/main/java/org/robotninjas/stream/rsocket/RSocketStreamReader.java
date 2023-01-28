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

import com.google.common.util.concurrent.AbstractService;
import io.micrometer.core.instrument.Metrics;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.micrometer.MicrometerRSocketInterceptor;
import io.rsocket.transport.ClientTransport;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.client.BatchProcessor;
import org.robotninjas.stream.client.StreamReader;
import org.robotninjas.stream.proto.ReadProxyClient;
import org.robotninjas.stream.security.AuthToken;
import reactor.core.Disposable;
import reactor.core.Disposables;

import static io.rsocket.frame.decoder.PayloadDecoder.ZERO_COPY;

@SuppressWarnings("UnstableApiUsage")
public class RSocketStreamReader extends AbstractService {
  private final ClientTransport transport;
  private final String streamName;

  private final Start start;
  private final BatchProcessor processor;
  private final AuthToken token;

  private Disposable subscription = Disposables.disposed();

  public RSocketStreamReader(
    ClientTransport transport,
    String streamName,
    Start start,
    BatchProcessor processor,
    AuthToken token
  ) {
    this.transport = transport;
    this.streamName = streamName;
    this.start = start;
    this.processor = processor;
    this.token = token;
  }

  @Override
  protected void doStart() {
    this.subscription = RSocketConnector.create()
      .payloadDecoder(ZERO_COPY)
      .resume(new Resume())
      .interceptors(registry ->
        registry.forRequester(new MicrometerRSocketInterceptor(Metrics.globalRegistry))
      ).connect(transport).flatMapMany(socket -> {
          var client = new ReadProxyClient(socket, Metrics.globalRegistry);
          return StreamReader.from(request ->
            client.read(request, Metadata.make(token))
          ).readBatch(streamName, start).flatMap(processor::process);
        }
      ).subscribe();
    notifyStarted();
  }

  @Override
  protected void doStop() {
    subscription.dispose();
    notifyStopped();
  }
}
