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
package org.robotninjas.stream.config;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import co.unruly.config.Configuration;
import co.unruly.config.ConfigurationSource;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.robotninjas.stream.Config;

import static co.unruly.config.Configuration.properties;
import static java.time.Duration.ZERO;

public class ConfigService extends AbstractScheduledService implements Config {
  private static final Duration InitialDelay = ZERO;

  private final ReloadableConfigurationSource reloadableConfigurationSource;
  private final Configuration configuration;
  private final Scheduler scheduler;

  public ConfigService(Scheduler scheduler, String file) {
    this.scheduler = scheduler;
    this.reloadableConfigurationSource = new ReloadableConfigurationSource(
      () -> properties(file)
    );

    this.configuration = Configuration.of(
      Configuration.systemProperties(),
      reloadableConfigurationSource,
      classpathProperties("driftwood.properties")
    );
  }

  public ConfigService(Duration schedule, String file) {
    this(Scheduler.newFixedDelaySchedule(InitialDelay, schedule), file);
  }

  private static ConfigurationSource classpathProperties(String file) {
    var props = new Properties();
    var url = ConfigService.class.getClassLoader().getResource(file);
    try (var stream = url.openStream()) {
      props.load(stream);
      return Configuration.properties(props);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void runOneIteration() {
    reloadableConfigurationSource.load();
  }

  @Override
  protected Scheduler scheduler() {
    return this.scheduler;
  }

  public Optional<String> get(String key) {
    return configuration.get(key);
  }

  private static class ReloadableConfigurationSource implements ConfigurationSource {
    private final Supplier<ConfigurationSource> factory;
    private final AtomicReference<ConfigurationSource> source =
      new AtomicReference<>(FIND_NOTHING);

    private ReloadableConfigurationSource(Supplier<ConfigurationSource> factory) {
      this.factory = factory;
    }

    @Override
    public String get(String key) {
      return source.get().get(key);
    }

    public void load() {
      source.set(factory.get());
    }
  }
}
