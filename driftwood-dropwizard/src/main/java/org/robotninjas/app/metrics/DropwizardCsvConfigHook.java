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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.Service;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.robotninjas.app.MetricsConfigHook;

public class DropwizardCsvConfigHook implements MetricsConfigHook {
  @SuppressWarnings("UnstableApiUsage")
  @Override
  public Service config(CompositeMeterRegistry registry, InetSocketAddress address) throws IOException {
    var dropwizardRegistry = new MetricRegistry();
    registry.add(new MyDropwizardMeterRegistry(dropwizardRegistry));

    var reporter = CsvReporter.forRegistry(dropwizardRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(File.createTempFile("metrics", "csv"));

    return new ScheduledReporterService(reporter);
  }
}
