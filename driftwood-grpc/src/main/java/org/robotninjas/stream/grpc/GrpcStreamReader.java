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
import com.google.common.util.concurrent.AbstractService;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.micrometer.core.instrument.Metrics;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.client.BatchProcessor;
import org.robotninjas.stream.client.StreamReader;
import org.robotninjas.stream.proto.ReactorReadProxyGrpc;
import org.robotninjas.stream.proto.ReactorReadProxyGrpc.ReactorReadProxyStub;
import org.robotninjas.stream.security.AuthToken;
import reactor.core.Disposable;
import reactor.core.Disposables;

@SuppressWarnings("UnstableApiUsage")
public class GrpcStreamReader extends AbstractService {
  private final ManagedChannel channel;
  private final String streamName;
  private final Start start;
  private final BatchProcessor processor;
  private final AuthToken authToken;

  private Disposable.Swap subscription = Disposables.swap();

  public GrpcStreamReader(
    ManagedChannel channel,
    String streamName,
    Start start,
    BatchProcessor processor,
    AuthToken authToken
  ) {
    this.channel = channel;
    this.streamName = streamName;
    this.start = start;
    this.processor = processor;
    this.authToken = authToken;
  }

  @Override
  protected void doStart() {
    ReactorReadProxyStub stub = ReactorReadProxyGrpc.newReactorStub(channel)
      .withCallCredentials(new AuthTokenCallCredentials(authToken))
      .withInterceptors(new MicrometerClientInterceptor(Metrics.globalRegistry));

    var disposable = StreamReader.from(stub::read)
      .readBatch(streamName, start)
      .flatMapSequential(processor::process)
      .subscribe();

    subscription.replace(disposable);

    notifyStarted();
  }

  @Override
  protected void doStop() {
    subscription.dispose();
    channel.shutdownNow();
    notifyStopped();
  }
}
