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

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import picocli.CommandLine.Option;

public class WriterCmd extends DlApp implements Callable<Integer> {
  @Option(names = {"--prometheus-address"}, defaultValue = "0.0.0.0:10001")
  InetSocketAddress metricsAddress;

  @Option(names = {"--write-count"})
  int writeCount = 1;

  @Option(names = {"--packet-size"})
  int packetSize = 1024;

  @Override
  public Integer call() throws Exception {
    runServiceWithNamespace(dlns ->
      new DLWriter(dlns, streamName, writeCount, packetSize)
    );
    return 0;
  }
}
