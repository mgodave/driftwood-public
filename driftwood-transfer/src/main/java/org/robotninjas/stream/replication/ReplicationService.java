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
package org.robotninjas.stream.replication;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.robotninjas.stream.transfer.Transfer;
import org.robotninjas.stream.transfer.TransferService;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

public class ReplicationService extends AbstractIdleService {
   private final Supplier<Flux<Set<ReplicationConfig>>> config;
   private final Supplier<Transfer> transfer;

   private Disposable subscription = Disposables.disposed();

   public ReplicationService(Supplier<Flux<Set<ReplicationConfig>>> config, Supplier<Transfer> transfer) {
      this.config = config;
      this.transfer = transfer;
   }

   @Override
   protected void startUp() throws Exception {
      this.subscription = config.get().buffer(2, 1)
         .skipLast(1)
         .reduce(Collections.<ReplicationConfig, TransferService>emptyMap(), (services, configs) -> {
            var previous = configs.get(0);
            var next = configs.get(1);

            Sets.difference(previous, next).forEach( s ->
               services.remove(s).stopAsync().awaitTerminated()
            );

            //noinspection ConstantConditions
            Sets.difference(next, previous).forEach( config ->
               services.put(config, new TransferService(
                  transfer.get(),
                  config.source(),
                  config.start(),
                  config.sink()
               )).startAsync()
            );

            return services;
         }).doOnNext(services -> {
            services.values().forEach(
               AbstractIdleService::stopAsync
            );
            services.clear();
         }).subscribe();
   }

   @Override
   protected void shutDown() throws Exception {
      subscription.dispose();
   }
}
