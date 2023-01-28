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

import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Test;
import picocli.CommandLine;

import static org.junit.Assert.assertTrue;

public class GrpcServerCommandsTest {

  @Test
  public void testBasicGrpcReadProxy() {
    var command = new GrpcReadProxyCommand() {{
      serverOptions(new MockServerOptions());
    }};

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs("--listen-port=8000");

    assertTrue(results.unmatched().isEmpty());
    assertTrue(results.errors().isEmpty());
  }

  @Test(expected = CommandLine.MissingParameterException.class)
  public void testGrpcReadProxyMissingListenPort() {
    var command = new GrpcReadProxyCommand() {{
      serverOptions(new MockServerOptions());
    }};

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs();

    assertTrue(results.unmatched().isEmpty());
  }

  @Test
  public void testBasicGrpcWriteProxy() {
    var command = new GrpcWriteProxyCommand() {{
      serverOptions(new MockServerOptions());
    }};

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs("--listen-port=8000");

    assertTrue(results.unmatched().isEmpty());
    assertTrue(results.errors().isEmpty());
  }

  @Test(expected = CommandLine.MissingParameterException.class)
  public void testGrpcWriteProxyMissingListenPort() {
    var command = new GrpcWriteProxyCommand() {{
      serverOptions(new MockServerOptions());
    }};

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs();

    assertTrue(results.unmatched().isEmpty());
  }

  /**
   * Make sure we never open an actual server socket
   */
  static class MockServerOptions extends GrpcServerOptions {
    MockServerOptions() {
      super(() -> InProcessServerBuilder.forName("ignore"));
    }
  }

}
