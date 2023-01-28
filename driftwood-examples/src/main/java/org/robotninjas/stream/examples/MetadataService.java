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
package org.robotninjas.stream.examples;

import java.sql.SQLException;
import java.util.function.Function;

import com.google.common.util.concurrent.AbstractService;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.jdbc.spring.DaoFactory;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTable;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamManager;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.proto.Stream;
import org.robotninjas.stream.proto.Subscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;

public class MetadataService extends AbstractService {

   private final StreamFactory streamFactory;
   private final Dao<StreamEntry, String> streamEntryDao;
   private final Dao<SubscriberEntry, String> subscriberEntryDao;
   private Disposable subscription = Disposables.disposed();

   public MetadataService(
      StreamFactory streamFactory,
      Dao<StreamEntry, String> streamEntryDao,
      Dao<SubscriberEntry, String> subscriberEntryDao
   ) {
      this.streamFactory = streamFactory;
      this.streamEntryDao = streamEntryDao;
      this.subscriberEntryDao = subscriberEntryDao;
   }

   static MetadataService create(StreamFactory streamFactory, ConnectionSource source) throws SQLException {
      return new MetadataService(
         streamFactory,
         DaoFactory.createDao(source, StreamEntry.class),
         DaoFactory.createDao(source, SubscriberEntry.class)
      );
   }

   @Override
   protected void doStart() {
      this.subscription = Disposables.composite(
         new StreamWriter(
            streamFactory.get(StreamName.of("stream_definitions")),
            new ProtoCodec<>(Stream.parser())
         ).process(),
         new SubscriberWriter(
            streamFactory.get(StreamName.of("subscriber_definitions")),
            new ProtoCodec<>(Subscriber.parser())
         ).process()
      );
      notifyStarted();
   }

   @Override
   protected void doStop() {
      subscription.dispose();
      notifyStopped();
   }

   public StreamEntry getStream(StreamName name) throws SQLException {
      return streamEntryDao.queryForId(name.name());
   }

   public SubscriberEntry getSubscriber(String subscriber) throws SQLException {
      var s = subscriberEntryDao.queryForId(subscriber);
      streamEntryDao.refresh(s.stream);
      return s;
   }

   @DatabaseTable(tableName = "streams")
   static class StreamEntry {

      @DatabaseField(id = true)
      String name;

      @DatabaseField
      String schema;

      public StreamEntry() {}

      public StreamEntry(String name) {
         this.name = name;
      }
   }

   @DatabaseTable(tableName = "subscribers")
   static class SubscriberEntry {

      @DatabaseField(id = true)
      String name;

      @DatabaseField(canBeNull = false, foreign = true)
      StreamEntry stream;

      public SubscriberEntry() {}
   }

   static record ProtoCodec<T extends Message>(Parser<T> parser) {
      Function<Record, Mono<T>> deserialize() {
         return record -> Mono.fromCallable(() ->
            parser.parseFrom(record.data())
         );
      }
   }

   abstract static class AbstractWriter<T extends Message> {
      private final StreamManager mgr;
      private final ProtoCodec<T> codec;

      protected AbstractWriter(StreamManager mgr, ProtoCodec<T> codec) {
         this.mgr = mgr;
         this.codec = codec;
      }

      abstract Mono<Void> doWrite(T record);

      Disposable process() {
         return mgr.read(Start.Oldest)
            .flatMap(codec.deserialize())
            .flatMap(this::doWrite)
            .subscribe();
      }
   }

   class SubscriberWriter extends AbstractWriter<Subscriber> {
      SubscriberWriter(StreamManager mgr, ProtoCodec<Subscriber> codec) {
         super(mgr, codec);
      }

      @Override
      Mono<Void> doWrite(Subscriber proto) {
         var entry = new SubscriberEntry();
         entry.name = proto.getName();
         entry.stream = new StreamEntry(proto.getStream());

         return Mono.fromCallable(() ->
            subscriberEntryDao.createOrUpdate(entry)
         ).then();
      }
   }

   class StreamWriter extends AbstractWriter<Stream> {
      StreamWriter(StreamManager mgr, ProtoCodec<Stream> codec) {
         super(mgr, codec);
      }

      @Override
      Mono<Void> doWrite(Stream proto) {
         var entry = new StreamEntry();
         entry.name = proto.getName();
         entry.schema = proto.getSchema();

         return Mono.fromCallable(() ->
            streamEntryDao.createOrUpdate(entry)
         ).then();
      }
   }
}
