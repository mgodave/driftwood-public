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
package org.robotninjas.stream.server;

import java.util.function.Predicate;

import org.apache.shiro.authz.AuthorizationException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.security.CallContext;
import org.robotninjas.stream.security.Permissions;
import org.robotninjas.stream.security.Principal;
import org.robotninjas.stream.security.Security;
import org.robotninjas.stream.test.BaseDriftwoodTest;
import org.robotninjas.stream.test.ReadOnlyStreamFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.robotninjas.stream.TestUtils.generateRecordsStream;
import static org.robotninjas.stream.security.Security.DefaultRefresh;
import static org.robotninjas.stream.server.ReadProxySupport.DefaultBufferTime;
import static org.robotninjas.stream.server.ReadProxySupport.MaxBufferPerResponse;
import static org.robotninjas.stream.server.ReadProxySupport.MaxBufferPerStream;

public class ReadProxySupportTest extends BaseDriftwoodTest {
  private static final CallContext MockContext = CallContext.Empty;

  static Predicate<Read.Response> expectedBatchSize(int size) {
    return response -> response.getSuccess().getBatchCount() == size;
  }

  static Runnable publishRecords(TestPublisher<Record.Stored> publisher, int n) {
    return () -> generateRecordsStream(n).forEach(publisher::next);
  }

  @Test
  public void testBufferRecordsBySize() {
    var publisher = TestPublisher.<Record.Stored>create();

    var streamFactory = ReadOnlyStreamFactory.create(Streamname, () -> publisher);
    var support = new ReadProxySupport(streamFactory, Security.Open);

    var request = Mono.just(makeReadRequest(Streamname));
    StepVerifier.withVirtualTime(() -> support.read(request, MockContext), 0)
      .expectSubscription()
      .thenRequest(1) // request one batch
      .then(publishRecords(publisher, 2 * MaxBufferPerResponse)) // publish two batches
      .expectNextMatches(expectedBatchSize(MaxBufferPerResponse)) // receive first batch
      .thenRequest(1)  // request second batch
      .expectNextMatches(expectedBatchSize(MaxBufferPerResponse)) // receive second batch
      .thenCancel()
      .verify();
  }

  @Test
  public void testBufferRecordsByTime() {
    var publisher = TestPublisher.<Record.Stored>create();

    var streamFactory = ReadOnlyStreamFactory.create(Streamname, () -> publisher);
    var support = new ReadProxySupport(streamFactory, Security.Open);

    var request = Mono.just(makeReadRequest(Streamname));
    StepVerifier.withVirtualTime(() -> support.read(request, MockContext), 0)
      .expectSubscription()
      .thenRequest(1) // request one batch
      .then(publishRecords(publisher, 2)) // publish two items, these will be in the first
      .thenAwait(DefaultBufferTime) // timeout the batching mechanism
      .then(publishRecords(publisher, 2)) // publish two more items, these will not be in the next batch
      .expectNextMatches(expectedBatchSize(2)) // get the first two
      .thenRequest(1) //get the next batch
      .thenAwait(DefaultBufferTime) // timeout the batching mechanism
      .expectNextMatches(expectedBatchSize(2)) // get the two after the timeout
      .thenCancel()
      .verify();
  }

  @Test
  public void testOnBackPressureBufferThenError() {
    var publisher = TestPublisher.<Record.Stored>create();

    var streamFactory = ReadOnlyStreamFactory.create(Streamname, () -> publisher);
    var support = new ReadProxySupport(streamFactory, Security.Open);

    var request = Mono.just(makeReadRequest(Streamname));
    StepVerifier.withVirtualTime(() -> support.read(request, MockContext), 0)
      .expectSubscription()
      .then(publishRecords(publisher, MaxBufferPerStream))
      .thenAwait(DefaultBufferTime)
      .verifyError(IllegalStateException.class);
  }

  @Test
  public void testFailReadIfPermissionsAreRevoked() {
    var streamFactory = ReadOnlyStreamFactory.create(Streamname, () ->
      Flux.fromStream(generateRecordsStream(2 * MaxBufferPerResponse))
    );
    
    var mockPrincipal = Mockito.mock(Principal.class);
    Mockito.when(mockPrincipal.check(Permissions.read(Streamname)))
      .thenReturn(Mono.just(mockPrincipal))
      .thenReturn(Mono.error(new AuthorizationException())); // no longer authorized

    Security mockSecurity = ctx -> Mono.just(mockPrincipal);

    var request = Mono.just(makeReadRequest(Streamname));
    var support = new ReadProxySupport(streamFactory, mockSecurity);

    StepVerifier.withVirtualTime(() -> support.read(request, MockContext), 0)
      .expectSubscription() // the first permission check happens on subscribe
      .thenRequest(1) // this will use the cached permission value
      .thenAwait(DefaultRefresh) // wait for the permission to refresh
      .thenRequest(1) // this request will fail after a new check
      .verifyError(AuthorizationException.class);

    // we're not testing the specific refresh rate here so we only want to make sure
    // that we call the check at least twice, once initially and one "later". We're ok
    // if it calls more but to guard against regression we should ensure that we are at least
    // checking that there is a success followed by a failure.
    Mockito.verify(mockPrincipal, atLeast(2)).check(Permissions.read(Streamname));
  }
}
