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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ServerBuilder;
import org.robotninjas.app.stream.Builder;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@SuppressWarnings("unused")
public class GrpcServerOptions {
  private final Builder<ServerBuilder<? extends ServerBuilder<?>>> builder = Builder.newBuilder();

  private Supplier<ServerBuilder<? extends ServerBuilder<?>>> init = () -> {
    throw new IllegalArgumentException("listen-port must be specified");
  };

  GrpcServerOptions() {}

  GrpcServerOptions(int listenPort) {
    init = () -> ServerBuilder.forPort(listenPort);
  }

  GrpcServerOptions(Supplier<ServerBuilder<? extends ServerBuilder<?>>> init) {
    this.init = init;
  }

  @Option(names = {"--listen-port"}, required = true)
  GrpcServerOptions withListenPort(int listenPort) {
    init = () ->
      ServerBuilder.forPort(listenPort);
    return this;
  }

  @Option(names = {"--handshake-timeout"})
  public GrpcServerOptions withHandshakeTimeout(long handshakeTimeout) {
    builder.add(builder ->
      builder.handshakeTimeout(handshakeTimeout, TimeUnit.MILLISECONDS)
    );
    return this;
  }

  @Option(names = {"--max-inbound-message-size"})
  public GrpcServerOptions withMaxInboundMessageSize(int maxInboundMessageSize) {
    builder.add(builder ->
      builder.maxInboundMessageSize(maxInboundMessageSize)
    );
    return this;
  }

  @Option(names = {"--max-inbound-metadata-size"})
  public GrpcServerOptions withMaxInboundMetadataSize(int maxInboundMetadataSize) {
    builder.add(builder ->
      builder.maxInboundMetadataSize(maxInboundMetadataSize)
    );
    return this;
  }

  @ArgGroup(exclusive = false)
  public GrpcServerOptions withTlsOptions(TlsOptions tlsOptions) {
    builder.add(builder ->
      builder.useTransportSecurity(tlsOptions.certChain(), tlsOptions.privateKey())
    );
    return this;
  }

  @Option(names = {"--enabled-compression-codecs"}, split = ",", defaultValue = "None")
  public GrpcServerOptions withEnabledCompressionCodecs(List<GrpcCompressionCodec> codecs) {
    builder.add(builder -> {
      // advertise compression codecs in order of preference
      var compressorRegistry = CompressorRegistry.newEmptyInstance();
      var decompressorRegistry = DecompressorRegistry.emptyInstance();
      for (var codec : codecs) {
        compressorRegistry.register(codec);
        decompressorRegistry = decompressorRegistry.with(codec, true);
      }
      return builder
        .compressorRegistry(compressorRegistry)
        .decompressorRegistry(decompressorRegistry);
    });
    return this;
  }

  public ServerBuilder<? extends ServerBuilder<?>> build() {
    return builder.build(init);
  }

  public static GrpcServerOptions builder(int listenPort) {
    return new GrpcServerOptions(listenPort);
  }

  public <T> T withServerBuilder(Function<ServerBuilder<? extends ServerBuilder<?>>, T> f) {
    return f.apply(builder.build(init));
  }
}
