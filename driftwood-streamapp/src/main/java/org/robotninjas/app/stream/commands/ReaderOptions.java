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

import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.client.BatchProcessor;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ReaderOptions {
  private String streamName = "messaging-stream-3";
  private Start start = Start.Newest;
  private BatchProcessor processor = BatchProcessor.Dots;
  private AuthOptions auth;

  public String streamName() {
    return streamName;
  }

  @Option(names = {"--stream-name"})
  public void streamName(String streamName) {
    this.streamName = streamName;
  }

  public Start start() {
    return start;
  }

  @ArgGroup
  public void start(StartOptions start) {
    if (start.atOffset.atOffset) {
      var offset = Offset.from(start.atOffset.offset);
      this.start = new Start.FromOffset(offset);
    } else if (start.fromNewest) {
      this.start = Start.Newest;
    } else if (start.fromOldest) {
      this.start = Start.Newest;
    }
  }

  public AuthOptions auth() {
    return auth;
  }

  @ArgGroup
  public void auth(AuthOptions auth) {
    this.auth = auth;
  }

  public BatchProcessor processor() {
    return processor;
  }

  public void processor(BatchProcessor processor) {
    this.processor = processor;
  }

  static class StartOptions {
    final static class StartAtOffset {
      @Option(names = {"--from-offset"})
      boolean atOffset;

      @Option(names = {"--offset"})
      byte[] offset;
    }

    @Option(names = {"--from-newest"})
    boolean fromNewest;

    @Option(names = {"--from-oldest"})
    boolean fromOldest;

    @ArgGroup(exclusive = false)
    StartAtOffset atOffset;
  }
}
