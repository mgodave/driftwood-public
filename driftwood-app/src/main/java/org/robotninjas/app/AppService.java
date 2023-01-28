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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.DiskSpaceMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import reactor.core.scheduler.Schedulers;

import static java.util.stream.Collectors.toUnmodifiableList;


@SuppressWarnings("UnstableApiUsage")
public class AppService extends AbstractService {
  private final ServiceManager main;
  private final String name;
  private final InetSocketAddress statsAddress;

  private ServiceManager statsServices;

  private AppService(Builder builder) {
    this.main = builder.main;
    this.name = builder.name;
    this.statsAddress = new InetSocketAddress(
      builder.statsAddress.getHost(),
      builder.statsAddress.getPort()
    );
  }

  public static Builder forService(Service main) {
    return new Builder(new ServiceManager(List.of(main)));
  }

  public static Builder forServices(ServiceManager main) {
    return new Builder(main);
  }

  @SuppressWarnings("WeakerAccess")
  protected String serviceName() {
    return name;
  }

  private ServiceManager initializeSystemMetrics() {
    Schedulers.enableMetrics();

    Metrics.globalRegistry.config().commonTags("application", serviceName());

    new ClassLoaderMetrics().bindTo(Metrics.globalRegistry);
    new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
    new JvmGcMetrics().bindTo(Metrics.globalRegistry);
    new JvmThreadMetrics().bindTo(Metrics.globalRegistry);
    new ProcessorMetrics().bindTo(Metrics.globalRegistry);
    new FileDescriptorMetrics().bindTo(Metrics.globalRegistry);
    new ProcessorMetrics().bindTo(Metrics.globalRegistry);
    new UptimeMetrics().bindTo(Metrics.globalRegistry);
    new LogbackMetrics().bindTo(Metrics.globalRegistry);
    new ProcessMemoryMetrics().bindTo(Metrics.globalRegistry);
    new ProcessThreadMetrics().bindTo(Metrics.globalRegistry);
    new DiskSpaceMetrics(new File("/")).bindTo(Metrics.globalRegistry);

    var metricsConfigHooks = ServiceLoader.load(MetricsConfigHook.class);
    return new ServiceManager(
      metricsConfigHooks.stream()
        .flatMap(hook -> {
          try {
            return Stream.of(hook.get().config(Metrics.globalRegistry, statsAddress));
          } catch (Exception e) {
            // TODO Log warning instead
            e.printStackTrace();
            return Stream.empty();
          }
        }).toList()
    );
  }

  @Override
  protected void doStart() {
    this.statsServices = initializeSystemMetrics();
    statsServices.startAsync().awaitHealthy();

    main.startAsync().addListener(new ServiceManager.Listener() {
      @Override
      public void healthy() {
        notifyStarted();
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  protected void doStop() {
    statsServices.stopAsync().awaitStopped();
    main.stopAsync().awaitStopped();
    notifyStopped();
  }

  public static class Builder {
    private final ServiceManager main;
    private String name;

    private HostAndPort statsAddress;

    private Builder(ServiceManager main) {
      this.main = main;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder metricsAddress(HostAndPort address) {
      this.statsAddress = address;
      return this;
    }

    public AppService build() {
      return new AppService(this);
    }
  }

}