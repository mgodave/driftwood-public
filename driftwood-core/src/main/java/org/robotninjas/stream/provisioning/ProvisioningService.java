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
package org.robotninjas.stream.provisioning;

import java.util.List;
import java.util.function.Function;

import com.google.common.util.concurrent.AbstractService;
import org.reactivestreams.Publisher;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ProvisioningService extends AbstractService {
   private static final StreamName DefaultProvisioningStream =
      StreamName.of("provisioning");

   private final StreamFactory streamFactory;
   private final StreamName provisioningStream;
   private final ProvisioningStore provisioningStore;

   public ProvisioningService(
      StreamFactory streamFactory,
      StreamName provisioningStream,
      ProvisioningStore provisioningStore
   ) {
      this.streamFactory = streamFactory;
      this.provisioningStream = provisioningStream;
      this.provisioningStore = provisioningStore;
   }

   public ProvisioningService(
      StreamFactory streamFactory,
      ProvisioningStore provisioningStore
   ) {
      this(
         streamFactory,
         DefaultProvisioningStream,
         provisioningStore
      );
   }

   @Override
   protected void doStart() {
      var startMono = provisioningStore.getLastWritten().map(Start.FromOffset::new);
      startMono.flatMapMany(start ->
         streamFactory.get(provisioningStream).read(start)
            .transform(MapToOperation.Op)
            .doOnNext(provisioningStore::write)
      );
   }

   @Override
   protected void doStop() {

   }

   enum MapToOperation implements Function<Flux<Record.Stored>, Publisher<Operation>> {
      Op;

      @Override
      public Publisher<Operation> apply(Flux<Record.Stored> in) {
         return null;
      }
   }

   record Member(String name) {}

   sealed interface Operation {
      Offset offset();
   }
   record Assign(StreamName stream, Member member, Offset offset) implements Operation {}
   record Members(List<Member> members, Offset offset) implements Operation {}

   interface ProvisioningStore {
      Mono<Offset> getLastWritten();
      Mono<Void> write(Operation op);
   }
}
