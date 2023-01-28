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
package org.robotninjas.app.dl;

import java.io.IOException;
import java.net.URI;
import java.util.function.Function;

import com.google.common.util.concurrent.Service;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.robotninjas.app.ServiceApplication;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.dlog.DLStreamFactory;
import picocli.CommandLine.Option;

@SuppressWarnings({"WeakerAccess", "CanBeFinal"})
class DlApp extends ServiceApplication {
  @Option(names = {"--dluri"})
  URI dluri = URI.create("distributedlog://localhost:7000/messaging/distributedlog");

  @Option(names = {"--stream-name"})
  String streamName = "messaging-stream-3";

  @Option(names = {"--dlconf"})
  URI dlconfuri = URI.create("classpath:org/robotninjas/app/dl/dl.conf");

  @SuppressWarnings("UnstableApiUsage")
  void runServiceWithNamespace(Function<Namespace, Service> f)
    throws ConfigurationException, IOException {

    var builder = NamespaceBuilder.newBuilder();

    // Create DistributedLog namespace
    var dlConfig = new DistributedLogConfiguration();

    if (dlconfuri != null) {
      dlConfig.loadConf(dlconfuri.toURL());
    }

    var namespaceBuilder = builder.conf(dlConfig).uri(dluri);

    try (var dlnamespace = namespaceBuilder.build()) {
      runServiceAsApplication(() -> f.apply(dlnamespace));
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  void runServiceWithStreamFactory(Function<StreamFactory, Service> f)
    throws ConfigurationException, IOException {

    runServiceWithNamespace(dlnamespace -> {
      try (var dlStreamFactory = new DLStreamFactory(dlnamespace)) {
        return f.apply(dlStreamFactory);
      }
    });
  }
}
