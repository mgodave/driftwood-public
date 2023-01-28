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

import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AbstractIdleService;
import io.github.ykayacan.hashing.api.HashFunction;
import io.github.ykayacan.hashing.api.NodeRouter;
import io.github.ykayacan.hashing.consistent.ConsistentNodeRouter;
import io.github.ykayacan.hashing.consistent.PhysicalNode;
import reactor.core.Disposable;
import reactor.core.Disposables;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class ResourceLocatorService extends AbstractIdleService implements ResourceLocator {
  private final Membership membership;
  private final Disposable.Swap subscription = Disposables.swap();
  private final Router router;

  public ResourceLocatorService(Membership membership) {
    this.membership = membership;
    this.router = new Router();
  }

  @Override
  public Node lookup(Resource resource) {
    return router.locate(resource);
  }

  @Override
  protected void startUp() {
    // a poor man's recursive loop, carry the previous
    // membership as input to the next iteration and
    // peel off the diff as output.
    record Accumulation(Set<Member> members, SetDiff diff) {}
    var init = new Accumulation(emptySet(), SetDiff.Empty);

    this.subscription.replace(
      membership.members().scan(init, (prev, next) ->
        new Accumulation(next, SetDiff.of(prev.members(), next))
      ).map(Accumulation::diff).subscribe(router::update)
    );
  }

  @Override
  protected void shutDown() {
    this.subscription.dispose();
  }

  private Set<PhysicalNode> asNodes(Set<Member> members) {
    return members.stream().map(m ->
      PhysicalNode.newBuilder(m.toString()).data(m).build()
    ).collect(toSet());
  }

  private Set<String> extractIds(Set<Member> members) {
    return members.stream().map(Member::id).collect(toSet());
  }

  @SuppressWarnings("UnstableApiUsage")
  record GuavaHashAdapter(com.google.common.hash.HashFunction fn) implements HashFunction {
    @Override
    public long hash(String key) {
      return fn.hashString(key, UTF_8).padToLong();
    }
  }

  record SetDiff(Set<Member> add, Set<Member> remove) {
    static SetDiff Empty = new SetDiff(emptySet(), emptySet());

    static SetDiff of(Set<Member> one, Set<Member> two) {
      return new SetDiff(
        Sets.difference(two, one),
        Sets.difference(one, two)
      );
    }
  }

  class Router {
    @SuppressWarnings("UnstableApiUsage")
    private final NodeRouter<PhysicalNode> ring =
      ConsistentNodeRouter.create(100, new GuavaHashAdapter(Hashing.murmur3_32()));

    private final ReadWriteLock ringLock =
      new ReentrantReadWriteLock();

    public Node locate(Resource resource) {
      var readLock = ringLock.readLock();
      readLock.lock();
      try {
        return ring.getNode(resource.toString())
          .flatMap(PhysicalNode::getData)
          .map(data -> new Node((Member) data))
          .get();
      } finally {
        readLock.unlock();
      }
    }

    public void update(SetDiff diff) {
      var writeLock = ringLock.writeLock();
      writeLock.lock();
      try {
        ring.removeNodes(extractIds(diff.remove()));
        ring.addNodes(asNodes(diff.add()));
      } finally {
        writeLock.unlock();
      }
    }
  }
}
