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

import java.io.IOException;

import com.be_hase.grpc.micrometer.MicrometerServerInterceptor;
import com.google.common.util.concurrent.AbstractService;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
public class GrpcService extends AbstractService {
  private static final Logger Log = LoggerFactory.getLogger(GrpcService.class);
  private final Server server;

  public GrpcService(ServerBuilder<? extends ServerBuilder<?>> serverBuilder, MeterRegistry metrics, BindableService... services) {
    var builder = serverBuilder.directExecutor()
      .intercept(new AuthServerInterceptor())
      .intercept(new MicrometerServerInterceptor(metrics));

    for (var service : services) {
      builder = builder.addService(service);
    }

    this.server = builder.build();
  }

  public GrpcService(ServerBuilder<? extends ServerBuilder<?>> serverBuilder, BindableService... services) {
    this(serverBuilder, Metrics.globalRegistry, services);
  }

  @Override
  protected void doStart() {
    try {
      server.start();
      notifyStarted();
    } catch (IOException e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    try {
      server.shutdown().awaitTermination();
      notifyStopped();
    } catch (InterruptedException e) {
      notifyFailed(e);
    }
  }
}
