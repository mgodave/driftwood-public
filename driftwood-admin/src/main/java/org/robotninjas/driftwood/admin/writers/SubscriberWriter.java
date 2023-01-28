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
package org.robotninjas.driftwood.admin.writers;

import com.j256.ormlite.dao.Dao;
import org.robotninjas.driftwood.admin.model.StreamEntry;
import org.robotninjas.driftwood.admin.model.SubscriberEntry;
import org.robotninjas.stream.StreamManager;
import org.robotninjas.stream.proto.Subscriber;
import reactor.core.publisher.Mono;

public class SubscriberWriter extends AbstractWriter<Subscriber> {
   private final Dao<SubscriberEntry, String> dao;

   public SubscriberWriter(
      StreamManager mgr,
      ProtoCodec<Subscriber> codec,
      Dao<SubscriberEntry, String> dao
   ) {
      super(mgr, codec);
      this.dao = dao;
   }

   @Override
   Mono<Void> doWrite(Subscriber proto) {
      var entry = new SubscriberEntry();
      entry.name = proto.getName();
      entry.stream = new StreamEntry(proto.getStream());
      entry.filter = proto.getFilter();

      return Mono.fromCallable(() ->
         dao.createOrUpdate(entry)
      ).then();
   }
}
