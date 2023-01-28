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

import java.util.function.Consumer;

import com.google.common.util.concurrent.AbstractService;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import reactor.core.Disposable;

@SuppressWarnings("UnstableApiUsage")
public class DLReader extends AbstractService {
  private final StreamFactory streamFactory;
  private final String streamName;
  private final Consumer<Record.Stored> consumer;

  private Disposable subscription;

  public DLReader(StreamFactory streamFactory, String streamName, Consumer<Record.Stored> consumer) {
    this.streamFactory = streamFactory;
    this.streamName = streamName;
    this.consumer = consumer;
  }

  public DLReader(StreamFactory streamFactory, String streamName) {
    this(streamFactory, streamName, ignore -> {});
  }

  @Override
  protected void doStart() {
    this.subscription = streamFactory.get(StreamName.of(streamName))
      .read(Start.Newest)
      .doOnNext(consumer)
      .subscribe();
    notifyStarted();
  }

  @Override
  protected void doStop() {
    subscription.dispose();
    notifyStopped();
  }
}