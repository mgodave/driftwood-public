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

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

class WriterOptions {
  private String streamName = "messaging-stream-3";
  private long numEvents = Long.MAX_VALUE;
  private int messageSize = 1024;
  private AuthOptions auth;

  @Option(names = {"--stream-name"})
  public void streamName(String streamName) {
    this.streamName = streamName;
  }

  public String streamName() {
    return streamName;
  }

  @Option(names = {"--num-events"})
  public void numEvents(long numEvents) {
    this.numEvents = numEvents;
  }

  public long numEvents() {
    return numEvents;
  }

  @Option(names = {"--message-size"})
  public void messageSize(int messageSize) {
    this.messageSize = messageSize;
  }

  public int messageSize() {
    return messageSize;
  }

  @ArgGroup
  public void auth(AuthOptions auth) {
    this.auth = auth;
  }

  public AuthOptions auth() {
    return auth;
  }

}
