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
package org.robotninjas.app.metrics;

import java.net.InetSocketAddress;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusRenameFilter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import org.robotninjas.app.MetricsConfigHook;

@SuppressWarnings("UnstableApiUsage")
public class PrometheusConfigHook implements MetricsConfigHook {
  @Override
  public Service config(CompositeMeterRegistry registry, InetSocketAddress address) {
    Metrics.globalRegistry.add(
        new PrometheusMeterRegistry(
            PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM));

    Metrics.globalRegistry.config().meterFilter(new PrometheusRenameFilter());

    return new AbstractService() {
      HTTPServer server;

      @Override
      protected void doStart() {
        try {
          this.server = new HTTPServer(address, CollectorRegistry.defaultRegistry, true);
          notifyStarted();
        } catch (Exception e) {
          notifyFailed(e);
        }
      }

      @Override
      protected void doStop() {
        try {
          this.server.stop();
          notifyStopped();
        } catch (Exception e) {
          notifyFailed(e);
        }
      }
    };
  }
}
