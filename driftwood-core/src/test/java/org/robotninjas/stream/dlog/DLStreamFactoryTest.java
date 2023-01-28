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
package org.robotninjas.stream.dlog;

import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.robotninjas.stream.Start;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.robotninjas.stream.StreamName.of;

@Ignore
public class DLStreamFactoryTest {

  private static final String StreamName = "stream1";

  @Test
  public void testStreamFactory() throws Exception {

    var mockNamespace = Mockito.mock(Namespace.class);
    var mockManager = Mockito.mock(DistributedLogManager.class);
    var mockReader = Mockito.mock(AsyncLogReader.class);

    when(mockNamespace.openLog(StreamName))
      .thenReturn(mockManager);

    when(mockManager.getAsyncLogReader(any()))
      .thenReturn(mockReader);

    var sf = new DLStreamFactory(mockNamespace);
    var stream = sf.get(of(StreamName)).read(Start.Newest);

    StepVerifier.create(stream)
      .expectError();
  }

}
