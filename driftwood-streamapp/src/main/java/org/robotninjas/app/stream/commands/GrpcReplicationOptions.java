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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.robotninjas.app.stream.Builder;
import picocli.CommandLine.Option;

@SuppressWarnings("unused")
public class GrpcReplicationOptions {
  /* This nonsense is all because I can'cause instantiate a ManagedChannelBuilder without
   * either an address or a withTarget. Because the object needs to be created by picocli
   * before and I can'cause guarantee the order in which the arguments are parsed, we need
   * to store everything as a function and delay the build until the last possible moment.
   * In this case that moment is when withChannel is called from the a call method somewhere
   * else.
   */
  private final Builder<ManagedChannelBuilder<? extends ManagedChannelBuilder<?>>> builder = Builder.newBuilder();

  private Supplier<ManagedChannelBuilder<? extends ManagedChannelBuilder<?>>> init = () -> {
    throw new IllegalArgumentException("withTarget must be specified");
  };

  GrpcReplicationOptions() {}

  GrpcReplicationOptions(String target) {
    this.init = () -> ManagedChannelBuilder.forTarget(target);
  }

  @Option(names = {"--with-target"}, required = true)
  GrpcReplicationOptions withTarget(String target) {
    init = () ->
      ManagedChannelBuilder.forTarget(target);
    return this;
  }

  @Option(names = {"--plaintext"})
  public GrpcReplicationOptions usePlaintext(boolean ignore) {
    builder.add(ManagedChannelBuilder::usePlaintext);
    return this;
  }

  @Option(names = {"--transport-security"})
  public GrpcReplicationOptions useTransportSecurity(boolean ignore) {
    builder.add(ManagedChannelBuilder::useTransportSecurity);
    return this;
  }

  @Option(names = {"--full-stream-decompression"}, split = ",", defaultValue = "None")
  public GrpcReplicationOptions enableFullStreamDecompression(List<GrpcCompressionCodec> codecs) {
    builder.add(builder -> {
      var compressorRegistry = CompressorRegistry.newEmptyInstance();
      var decompressorRegistry = DecompressorRegistry.emptyInstance();
      for (var codec : codecs) {
        compressorRegistry.register(codec);
        decompressorRegistry = decompressorRegistry.with(codec, true);
      }

      return builder
        .enableFullStreamDecompression()
        .compressorRegistry(compressorRegistry)
        .decompressorRegistry(decompressorRegistry);
    });
    return this;
  }

  @Option(names = {"--idle-timeout"})
  public GrpcReplicationOptions withIdleTimeout(long idleTimeout) {
    builder.add(builder ->
      builder.idleTimeout(idleTimeout, TimeUnit.MILLISECONDS)
    );
    return this;
  }

  @Option(names = {"--max-inbound-message-size"})
  public GrpcReplicationOptions withMaxInboundMessageSize(int maxInboundMessageSize) {
    builder.add(builder ->
      builder.maxInboundMessageSize(maxInboundMessageSize)
    );
    return this;
  }

  @Option(names = {"--max-inbound-metadata-size"})
  public GrpcReplicationOptions withMaxInboundMetadataSize(int maxInboundMetadataSize) {
    builder.add(builder ->
      builder.maxInboundMetadataSize(maxInboundMetadataSize)
    );
    return this;
  }

  @Option(names = {"--keep-alive-time"})
  public GrpcReplicationOptions withKeepAliveTime(long keepAliveTime) {
    builder.add(builder ->
      builder.keepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS)
    );
    return this;
  }

  @Option(names = {"--keep-alive-timeout"})
  public GrpcReplicationOptions withKeepAliveTimeout(long keepAliveTimeout) {
    builder.add(builder ->
      builder.keepAliveTimeout(keepAliveTimeout, TimeUnit.MILLISECONDS)
    );
    return this;
  }

  @Option(names = {"--keep-alive-without-calls"}, negatable = true)
  public GrpcReplicationOptions keepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
    builder.add(builder ->
      builder.keepAliveWithoutCalls(keepAliveWithoutCalls)
    );
    return this;
  }

  @Option(names = {"--max-retry-attempts"})
  public GrpcReplicationOptions withMaxRetryAttempts(int maxRetryAttempts) {
    builder.add(builder ->
      builder.maxRetryAttempts(maxRetryAttempts)
    );
    return this;
  }

  @Option(names = {"--max-hedged-attempts"})
  public GrpcReplicationOptions withMaxHedgedAttempts(int maxHedgedAttempts) {
    builder.add(builder ->
      builder.maxHedgedAttempts(maxHedgedAttempts)
    );
    return this;
  }

  @Option(names = {"--retry-buffer-size"})
  public GrpcReplicationOptions withRetryBufferSize(long retryBufferSize) {
    builder.add(builder ->
      builder.retryBufferSize(retryBufferSize)
    );
    return this;
  }

  @Option(names = {"--pre-rpc-buffer-limit"})
  public GrpcReplicationOptions withPerRpcBufferLimit(long perRpcBufferLimit) {
    builder.add(builder ->
      builder.perRpcBufferLimit(perRpcBufferLimit)
    );
    return this;
  }

  @Option(names = {"--retry"})
  public GrpcReplicationOptions retry(boolean ignore) {
    builder.add(ManagedChannelBuilder::enableRetry);
    return this;
  }

  public ManagedChannel build() {
    return builder.build(init).build();
  }

  public static GrpcReplicationOptions builder(String target) {
    return new GrpcReplicationOptions(target);
  }

  <T> T withChannel(Function<ManagedChannel, T> f) {
      return f.apply(builder.build(init).build());
  }
}
