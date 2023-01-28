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
package org.robotninjas.driftwood.admin.service;

import java.nio.file.Path;
import java.sql.SQLException;

import com.google.common.util.concurrent.AbstractService;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.jdbc.spring.DaoFactory;
import com.j256.ormlite.support.ConnectionSource;
import org.robotninjas.driftwood.admin.MetadataService;
import org.robotninjas.driftwood.admin.writers.ProtoCodec;
import org.robotninjas.driftwood.admin.model.StreamEntry;
import org.robotninjas.driftwood.admin.model.SubscriberEntry;
import org.robotninjas.driftwood.admin.writers.StreamWriter;
import org.robotninjas.driftwood.admin.writers.SubscriberWriter;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.SubscriberName;
import org.robotninjas.stream.proto.Stream;
import org.robotninjas.stream.proto.Subscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;

public class MetadataServiceImpl extends AbstractService implements MetadataService {

   private final StreamFactory streamFactory;
   private final Dao<StreamEntry, String> streamEntryDao;
   private final Dao<SubscriberEntry, String> subscriberEntryDao;
   private final Path storage;
   private Disposable subscription = Disposables.disposed();

   public MetadataServiceImpl(
      StreamFactory streamFactory,
      Path storage,
      Dao<StreamEntry, String> streamEntryDao,
      Dao<SubscriberEntry, String> subscriberEntryDao
   ) {
      this.streamFactory = streamFactory;
      this.streamEntryDao = streamEntryDao;
      this.subscriberEntryDao = subscriberEntryDao;
      this.storage = storage;
   }

   static MetadataServiceImpl create(StreamFactory streamFactory, ConnectionSource source, Path storage) throws SQLException {
      return new MetadataServiceImpl(
         streamFactory,
         storage,
         DaoFactory.createDao(source, StreamEntry.class),
         DaoFactory.createDao(source, SubscriberEntry.class)
      );
   }

   @Override
   protected void doStart() {
      this.subscription = Disposables.composite(
         new StreamWriter(
            streamFactory.get(StreamName.of("stream_definitions")),
            new ProtoCodec<>(Stream.parser()),
            streamEntryDao
         ).process(),
         new SubscriberWriter(
            streamFactory.get(StreamName.of("subscriber_definitions")),
            new ProtoCodec<>(Subscriber.parser()),
            subscriberEntryDao
         ).process()
      );
      notifyStarted();
   }

   @Override
   protected void doStop() {
      subscription.dispose();
      notifyStopped();
   }

   @Override
   public Mono<StreamEntry> getStream(StreamName name) {
      return Mono.fromCallable(() ->
        streamEntryDao.queryForId(name.name())
      );
   }

   @Override
   public Mono<SubscriberEntry> getSubscriber(SubscriberName subscriber) {
      return Mono.fromCallable(() -> {
         var s = subscriberEntryDao.queryForId(subscriber.name());
         streamEntryDao.refresh(s.stream);
         return s;
      });
   }

}
