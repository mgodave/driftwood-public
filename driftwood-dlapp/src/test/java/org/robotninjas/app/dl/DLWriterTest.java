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

import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.junit.Test;

import static com.google.common.util.concurrent.Service.State.NEW;
import static com.google.common.util.concurrent.Service.State.TERMINATED;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DLWriterTest {

  private static final String StreamName = "stream1";

  @Test
  public void testDlWriterFinishesAfterNWrites() throws Exception {
    var mockNamespace = mock(Namespace.class);
    var mockManager = mock(DistributedLogManager.class);
    when(mockNamespace.openLog(StreamName))
      .thenReturn(mockManager);

    var mockLogWriter = mock(AsyncLogWriter.class);
    when(mockManager.openAsyncLogWriter())
      .thenReturn(completedFuture(mockLogWriter));
    when(mockManager.asyncClose())
      .thenReturn(completedFuture(null));

    when(mockLogWriter.write(any()))
      .thenReturn(completedFuture(DLSN.InitialDLSN));

    final var numWrites = 10;
    final var numBytes = 1;

    var dlwriter = new DLWriter(
      mockNamespace,
      StreamName,
      numWrites,
      numBytes
    );

    assertSame(dlwriter.state(), NEW);

    dlwriter.startAsync().awaitTerminated();

    assertSame(dlwriter.state(), TERMINATED);
    
    verify(mockLogWriter, times(numWrites)).write(any());
//    verify(mockLogWriter, times(1)).asyncClose();
    verify(mockManager, times(1)).asyncClose();
    verify(mockNamespace, never()).close();
  }

}
