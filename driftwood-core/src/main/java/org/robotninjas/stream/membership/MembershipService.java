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
package org.robotninjas.stream.membership;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.AbstractService;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamManager;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.TxId;
import org.robotninjas.stream.internal.proto.Membership;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static java.time.temporal.ChronoUnit.MILLIS;

@SuppressWarnings("UnusedVariable")
public class MembershipService extends AbstractService implements GroupMembership {
   private static final Duration DafaultUpdateInterval = Duration.ofMillis(100);
   private static final Duration DefaultFailureInterval = DafaultUpdateInterval.multipliedBy(2);

   private final StreamFactory factory;
   private final Clock clock;
   private final Member localMember;
   private final StreamName membershipStream;
   private final Duration updateInterval;
   private final Duration failureInterval;
   private final AtomicReference<Set<Member>> membership = new AtomicReference<>();
   private final Scheduler scheduler = Schedulers.single();

   private final Disposable.Swap disposable = Disposables.swap();

   MembershipService(
      StreamFactory factory,
      Clock clock,
      Member localMember,
      StreamName membershipStream,
      Duration updateInterval,
      Duration failureInterval
   ) {
      this.factory = factory;
      this.clock = clock;
      this.localMember = localMember;
      this.membershipStream = membershipStream;
      this.updateInterval = updateInterval;
      this.failureInterval = failureInterval;
   }

   public MembershipService(
      StreamFactory factory,
      Member localMember,
      StreamName membershipStream,
      Duration updateInterval
   ) {
      this(factory, Clock.systemUTC(), localMember, membershipStream, updateInterval, DefaultFailureInterval);
   }

   public MembershipService(StreamFactory factory, Member localMember, StreamName membershipStream) {
      this(factory, Clock.systemUTC(), localMember, membershipStream, DefaultFailureInterval, DefaultFailureInterval);
   }

   private static Mono<Message> toMessage(Record.Stored rec) {
      return Mono.fromCallable(() ->
         Membership.parseFrom(rec.data())
      ).flatMap(parsed ->
         switch (parsed.getMessageCase()) {
            case UPDATE -> {
               var update = parsed.getUpdate();
               var member = new Member(update.getMember().getMember());
               yield Mono.just(new Update(member));
            }
            default -> Mono.error(new Exception("MessageNotSet"));
         }
      );
   }

   static Record toRecord(Message message) {
      Membership.Builder builder = Membership.newBuilder();
      if (message instanceof Update update) {
         builder.setUpdate(
            Membership.Update.newBuilder().setMember(
               Membership.Member.newBuilder()
                  .setMember(update.member().name())
            )
         );
      }

      var built = builder.build();
      return new MessageRecord(new Record.SimpleRecord(
         ByteBuffer.wrap(built.toByteArray()), TxId.Empty
      ), message);
   }

   @Override
   public Set<Member> current() {
      return membership.get();
   }

   @Override
   protected void doStop() {
      disposable.dispose();
      scheduler.dispose();
      notifyStopped();
   }

   @Override
   protected void doStart() {
      StreamManager manager = factory.get(membershipStream);

      Map<Member, Instant> initial = new HashMap<>();

      // Listen for updates from the membership stream
      var updates = manager.read(Start.Newest)
         .flatMap(MembershipService::toMessage)
         .scan(initial, (current, msg) -> {
            Map<Member, Instant> next = new HashMap<>(current);
            if (msg instanceof Update update) {
               var now = clock.instant();
               next.put(update.member(), now);
            }
            return next;
         });

      // Make decisions about whether current members are still active
      Flux<DetectorState> detector =
         updates.sample(updateInterval).map(update -> {
            var healthy = new HashSet<Member>();
            var failed = new HashSet<Member>();
            var now = Instant.now(clock);
            update.forEach((streamname, lastUpdate) -> {
               var passed = lastUpdate.until(now, MILLIS);
               if (passed >= failureInterval.toMillis()) {
                  failed.add(streamname);
               } else {
                  healthy.add(streamname);
               }
            });
            return new DetectorState(healthy, failed);
         });

      // Update local state
      Flux<Set<Member>> healthy = detector.map(
         DetectorState::healthy
      ).distinctUntilChanged().publish().refCount();

      // Send a heartbeat to the membership stream
      Flux<Offset> heartbeat = manager.write(
         Flux.interval(DafaultUpdateInterval).map(i ->
            toRecord(new Update(localMember))
         )
      );

      // setup a disposable to tear it all down
      disposable.replace(
         Disposables.composite(
            heartbeat.subscribe(),
            healthy.subscribe(
               membership::set
            ),
            () -> {
               try {
                  manager.close();
               } catch (Exception ignore) {
               }
            }
         )
      );

      notifyStarted();
   }

   private sealed interface Message {
      Member member();
   }

   private record Update(Member member) implements Message {}

   private record DetectorState(Set<Member> healthy, Set<Member> failed) {}

   // Store the message with the record so we can debug
   private record MessageRecord(Record record, Message message) implements Record.StoredAdapter {}
}
