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
package org.robotninjas.stream.cluster;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.google.common.util.concurrent.AbstractIdleService;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class ClusterService extends AbstractIdleService implements Membership, Broadcast {
  private final Metadata metadata;
  private final MembershipHandler handler;
  private final Flux<Set<Member>> members;
  private final Disposable subscription;
  private final MembershipConfig config;
  private Cluster cluster;

  public ClusterService(MembershipConfig config) {
    this.metadata = new Metadata(config.metadataNumVnodes());
    this.config = config;
    this.handler = new MembershipHandler();

    var connectable = Flux.<MembershipEvent>push(emitter ->
      handler.register(emitter::next)
    ).scanWith(Collections::<Member>emptySet, (members, event) -> {
      var next = new HashSet<>(members);
      if (event.isAdded()) {
        next.add(new MemberImpl(event.member()));
      } else if (event.isRemoved()) {
        next.remove(new MemberImpl(event.member()));
      }
      return Collections.unmodifiableSet(next);
    }).replay(1);

    this.subscription = connectable.connect();
    this.members = connectable;
  }

  @Override
  protected void startUp() {
    this.cluster = new ClusterImpl()
      .config(opts -> clusterConfig(config))
      .config(opts -> opts.metadata(metadata))
      .handler(ignore -> handler)
      .transportFactory(TcpTransportFactory::new)
      .startAwait();
  }

  @Override
  public Flux<Set<Member>> members() {
    return members;
  }

  @Override
  public Member local() {
    return new MemberImpl(cluster.member());
  }

  @Override
  public Mono<Void> broadcast(byte[] message) {
    var msg = io.scalecube.cluster.transport.api.Message.fromData(message);
    return Flux.fromStream(
      cluster.members().stream().map(member ->
        cluster.send(member, msg)
      )).flatMap(identity()).then();
  }

  @Override
  public Flux<Message> receive() {
    return null;
  }

  @Override
  public Optional<Metadata> metadata(Member member) {
    var m = cluster.member(member.id());
    return m.flatMap(cluster::metadata);
  }

  @Override
  protected void shutDown() throws Exception {
    this.subscription.dispose();
    this.cluster.shutdown();
  }

  private ClusterConfig clusterConfig(MembershipConfig config) {
    var clusterConfig = ClusterConfig.defaultConfig();

    config.membershipMetadataTimeout()
      .ifPresent(clusterConfig::metadataTimeout);

    config.membershipExternalHost()
      .ifPresent(clusterConfig::externalHost);

    config.membershipExternalPort()
      .ifPresent(clusterConfig::externalPort);

    config.membershipMemberAlias()
      .ifPresent(clusterConfig::memberAlias);

    clusterConfig.membership(opts -> {
      config.membershipSyncInterval()
        .ifPresent(opts::syncInterval);

      config.membershipSyncTimeout()
        .ifPresent(opts::syncTimeout);

      config.membershipSuspicionMultiplier()
        .ifPresent(opts::suspicionMult);

      opts.seedMembers(
        config.membershipSeedMembers().stream()
          .map(Address::from)
          .collect(toList())
      );

      config.membershipNamespace()
        .ifPresent(opts::namespace);

      config.membershipRemovedMemberHistorySize()
        .ifPresent(opts::removedMembersHistorySize);

      return opts;
    });

    clusterConfig.transport(opts -> {
      config.membershipPort()
        .ifPresent(opts::port);

      config.membershipClientSecured()
        .ifPresent(opts::clientSecured);

      config.membershipConnectTimeout()
        .ifPresent(opts::connectTimeout);

      config.membershipMaxFrameLength()
        .ifPresent(opts::maxFrameLength);

      return opts;
    });

    clusterConfig.gossip(opts -> {
      config.membershipGossipInterval()
        .ifPresent(opts::gossipInterval);

      config.membershipGossipFanout()
        .ifPresent(opts::gossipFanout);

      config.membershipGossipRepeatMultiplier()
        .ifPresent(opts::gossipRepeatMult);

      config.membershipGossipSegmentationThreshold()
        .ifPresent(opts::gossipSegmentationThreshold);

      return opts;
    });

    clusterConfig.failureDetector(opts -> {
      config.membershipFailureDetectorPingInterval()
        .ifPresent(opts::pingInterval);

      config.membershipFailureDetectorPingTimeout()
        .ifPresent(opts::pingTimeout);

      config.membershipFailureDetectorPingRequestMembers()
        .ifPresent(opts::pingReqMembers);

      return opts;
    });

    return clusterConfig;

  }

  record MemberImpl(String id, io.scalecube.cluster.Member member) implements Member, Serializable {
    MemberImpl(io.scalecube.cluster.Member member) {
      this(member.id(), member);
    }
  }

  static private final class MembershipHandler implements ClusterMessageHandler {
    private final CopyOnWriteArrayList<Consumer<MembershipEvent>> consumers =
      new CopyOnWriteArrayList<>();

    public void register(Consumer<MembershipEvent> consumer) {
      consumers.add(consumer);
    }

    public void unregister(Consumer<MembershipEvent> consumer) {
      consumers.remove(consumer);
    }

    @Override
    public void onMembershipEvent(MembershipEvent event) {
      consumers.forEach(consumer -> consumer.accept(event));
    }
  }

  static private final class MessageHandler implements ClusterMessageHandler {
    Consumer<Message> consumer;

    public MessageHandler(Consumer<Message> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void onMessage(io.scalecube.cluster.transport.api.Message message) {
//      var sender = new MemberImpl();
//      byte[] data = message.data();
//      consumer.accept(new Message(sender, data));
    }
  }
}
