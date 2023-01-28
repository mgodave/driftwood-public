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

import java.time.Duration;
import java.util.Arrays;

import com.google.common.util.concurrent.AbstractService;
import io.micrometer.core.instrument.Metrics;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.ipc.RequestHandlingRSocket;
import io.rsocket.micrometer.MicrometerDuplexConnectionInterceptor;
import io.rsocket.micrometer.MicrometerRSocketInterceptor;
import io.rsocket.plugins.InterceptorRegistry;
import io.rsocket.rpc.AbstractRSocketService;
import io.rsocket.transport.ServerTransport;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;

import static io.rsocket.frame.decoder.PayloadDecoder.ZERO_COPY;

@SuppressWarnings("UnstableApiUsage")
public class RSocketService extends AbstractService {
  private static final Resume ResumeStrategy = new Resume()
    .cleanupStoreOnKeepAlive()
    .sessionDuration(Duration.ofMinutes(1));

  private final ServerTransport<?> transport;
  private final AbstractRSocketService[] services;
  private Disposable.Swap subscription = Disposables.swap();

  public RSocketService(ServerTransport<?> transport, AbstractRSocketService... services) {
    this.services = services;
    this.transport = transport;
  }

  private static SocketAcceptor forServices(AbstractRSocketService... services) {
    return (ConnectionSetupPayload setup, RSocket sendingSocket) -> {
      var handler = new RequestHandlingRSocket();
      Arrays.stream(services).forEach(handler::withEndpoint);
      return Mono.just(handler);
    };
  }

  private void registerInterceptors(InterceptorRegistry registry) {
    registry.forConnection(new MicrometerDuplexConnectionInterceptor(Metrics.globalRegistry));
    registry.forResponder(new MicrometerRSocketInterceptor(Metrics.globalRegistry));
  }

  @Override
  protected void doStart() {
    var disposable = RSocketServer.create()
      .payloadDecoder(ZERO_COPY)
      .acceptor(forServices(services))
      .resume(ResumeStrategy)
      .interceptors(this::registerInterceptors)
      .bind(transport)
      .doOnError(this::notifyFailed)
      .subscribe();
    subscription.replace(disposable);
    notifyStarted();
  }

  @Override
  protected void doStop() {
    subscription.dispose();
    notifyStopped();
  }
}
