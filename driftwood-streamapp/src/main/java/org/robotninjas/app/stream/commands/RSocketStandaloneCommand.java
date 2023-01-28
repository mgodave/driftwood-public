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

import java.util.concurrent.Callable;

import org.robotninjas.stream.rsocket.RSocketReadProxy;
import org.robotninjas.stream.rsocket.RSocketReplicator;
import org.robotninjas.stream.rsocket.RSocketService;
import org.robotninjas.stream.rsocket.RSocketWriteProxy;
import picocli.CommandLine.Command;

@Command(name = "rsocket", abbreviateSynopsis = true)
public class RSocketStandaloneCommand extends RSocketServerCommand implements Callable<Integer> {
  @Override
  public Integer call() throws Exception {
    runStandaloneWithStreamFactory(streamFactory ->
      new RSocketService(transport(),
        RSocketReplicator.server(streamFactory, security()),
        RSocketReadProxy.server(streamFactory, security()),
        RSocketWriteProxy.server(streamFactory, security())
      )
    );
    return 0;
  }
}
