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

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.local.LocalServerTransport;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.junit.Test;
import picocli.CommandLine;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RSocketServerCommandsTest {
  @Test
  public void testBasicRSocketReadProxy() {
    var command = new MockedReadProxyCommand();

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs("--listen-address=127.0.0.1:8000");

    assertTrue(results.unmatched().isEmpty());
    assertTrue(results.errors().isEmpty());
  }

  @Test(expected = CommandLine.MissingParameterException.class)
  public void testRSocketReadProxyMissingListenPort() {
    var command = new MockedReadProxyCommand();

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs();

    assertTrue(results.unmatched().isEmpty());
  }

  @Test
  public void testBasicRSocketWriteProxy() {
    var command = new MockedWriteProxyCommand();

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs("--listen-address=127.0.0.1:8000");

    assertTrue(results.unmatched().isEmpty());
    assertTrue(results.errors().isEmpty());
  }

  @Test(expected = CommandLine.MissingParameterException.class)
  public void testRSocketWriteProxyMissingListenPort() {
    var command = new MockedWriteProxyCommand();

    var cl = new picocli.CommandLine(command);
    var results = cl.parseArgs();

    assertTrue(results.unmatched().isEmpty());
  }

  static class MockedReadProxyCommand extends RSocketReadProxyCommand {
    @Override
    protected NamespaceBuilder namespace() throws IOException {
      var mockNamespace = mock(Namespace.class);
      var mockBuilder = mock(NamespaceBuilder.class);
      when(mockBuilder.build()).thenReturn(mockNamespace);
      return mockBuilder;
    }

    @Override
    protected ServerTransport<Closeable> transport() {
      return LocalServerTransport.create("ignore");
    }
  }

  static class MockedWriteProxyCommand extends RSocketWriteProxyCommand {
    @Override
    protected NamespaceBuilder namespace() throws IOException {
      var mockNamespace = mock(Namespace.class);
      var mockBuilder = mock(NamespaceBuilder.class);
      when(mockBuilder.build()).thenReturn(mockNamespace);
      return mockBuilder;
    }

    @Override
    protected ServerTransport<Closeable> transport() {
      return LocalServerTransport.create("ignore");
    }
  }
}
