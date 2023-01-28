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
package org.robotninjas.app;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import picocli.CommandLine.Option;

@SuppressWarnings("UnstableApiUsage")
public class ServiceApplication {
  private HostAndPort metricsAddress = HostAndPort.fromString("");

  @SuppressWarnings("unused")
  public HostAndPort metricsAddress() {
    return metricsAddress;
  }

  @Option(names = {"--prometheus-address"}, defaultValue = "0.0.0.0:10001", converter = {HostAndPortConverter.class})
  public void metricsAddress(HostAndPort metricsAddress) {
    this.metricsAddress = metricsAddress;
  }

  protected void runServiceAsApplication(Supplier<Service> f) {
    var app = AppService.forService(f.get())
      .metricsAddress(metricsAddress)
      .name("server")
      .build();

    Runtime.getRuntime().addShutdownHook(
      new Thread(app::stopAsync)
    );

    app.startAsync().awaitTerminated();
  }

  protected void runServiceManagerAsApplication(Supplier<ServiceManager> f) {
    var app = AppService.forServices(f.get())
      .metricsAddress(metricsAddress)
      .name("server")
      .build();

    Runtime.getRuntime().addShutdownHook(
      new Thread(app::stopAsync)
    );

    app.startAsync()
      .awaitTerminated();
  }
}
