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
package org.robotninjas.app.stream.commands;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.function.Function;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.robotninjas.app.ServiceApplication;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.bookkeeper.LocalDistributedLogService;
import org.robotninjas.stream.dlog.DLStreamFactory;
import org.robotninjas.stream.security.Security;
import picocli.CommandLine.Option;

@SuppressWarnings({"UnstableApiUsage", "unused"})
abstract class BaseServerCommand extends ServiceApplication {
  @Option(names = {"--dluri"})
  private URI dluri = URI.create("distributedlog://localhost:7000/messaging/distributedlog");

  @Option(names = {"--dlconf"})
  private URI dlconfuri;

  @Option(names = {"--securityconf"})
  private String securityConf;

  public void dluri(URI dluri) {
    this.dluri = dluri;
  }

  public void dlconfuri(URI dlconfuri) {
    this.dlconfuri = dlconfuri;
  }

  public void securityConf(String securityConf) {
    this.securityConf = securityConf;
  }

  /**
   * Exposed for testing so we can mock a builder
   */
  protected NamespaceBuilder namespace()
        throws ConfigurationException, IOException {

      var builder = NamespaceBuilder.newBuilder();

      // Create DistributedLog namespace
      var dlConfig = new DistributedLogConfiguration();
      if (dlconfuri != null) {
        dlConfig.loadConf(dlconfuri.toURL());
      }

      return builder.conf(dlConfig).uri(dluri);
  }

  protected NamespaceBuilder namespace(URI uri)
    throws ConfigurationException, IOException {

    var builder = NamespaceBuilder.newBuilder();

    // Create DistributedLog namespace
    var dlConfig = new DistributedLogConfiguration();
    if (dlconfuri != null) {
      dlConfig.loadConf(dlconfuri.toURL());
    }

    return builder.conf(dlConfig).uri(uri);
  }

  protected Security security() {
    var factory = new IniSecurityManagerFactory(securityConf);
    return new Security.ShiroSecurity(factory.getInstance());
  }

  private void runServiceWithNamespace(Function<Namespace, Service> f)
    throws IOException, ConfigurationException {

    try (var dlnamespace = namespace().build()) {
      runServiceAsApplication(() -> f.apply(dlnamespace));
    }
  }

  private void runServiceManagerWithNamespace(Function<Namespace, ServiceManager> f)
    throws IOException, ConfigurationException {

    try (var dlnamespace = namespace().build()) {
      runServiceManagerAsApplication(() -> f.apply(dlnamespace));
    }
  }

  private void runStandaloneWithNamespace(Function<Namespace, Service> f)
    throws Exception {

    var confFile = getClass().getClassLoader().getResource("default_bk.conf");

    var serverConf = new ServerConfiguration();
    serverConf.loadConf(confFile);

    var localDL = LocalDistributedLogService.standalone(serverConf);
    localDL.startAsync().awaitRunning();

    try (var dlnamespace = namespace(localDL.uri()).build()) {
      runServiceManagerAsApplication(
        () -> new ServiceManager(List.of(
          localDL, f.apply(dlnamespace)
        ))
      );
    }
  }

  void runStandaloneWithStreamFactory(Function<StreamFactory, Service> f)
    throws Exception {
    runStandaloneWithNamespace(dlnamespace -> {
      try (var dlStreamFactory = new DLStreamFactory(dlnamespace)) {
        return f.apply(dlStreamFactory);
      }
    });
  }

  void runServiceWithStreamFactory(Function<StreamFactory, Service> f)
    throws ConfigurationException, IOException {
    runServiceWithNamespace(dlnamespace -> {
      try (var dlStreamFactory = new DLStreamFactory(dlnamespace)) {
        return f.apply(dlStreamFactory);
      }
    });
  }

  void runServiceManagerWithStreamFactory(Function<StreamFactory, ServiceManager> f)
    throws ConfigurationException, IOException {
    runServiceManagerWithNamespace(dlnamespace -> {
      try (var dlStreamFactory = new DLStreamFactory(dlnamespace)) {
        return f.apply(dlStreamFactory);
      }
    });
  }

  <T> T withNamespace(Function<Namespace, T> f)
    throws IOException, ConfigurationException {
    try (var dlnamespace = namespace().build()) {
      return f.apply(dlnamespace);
    }
  }

  <T> T withStreamFactory(Function<StreamFactory, T> f)
    throws IOException, ConfigurationException {
    return withNamespace(dlnamespace -> {
      try (var dlStreamFactory = new DLStreamFactory(dlnamespace)) {
        return f.apply(dlStreamFactory);
      }
    });
  }
}
