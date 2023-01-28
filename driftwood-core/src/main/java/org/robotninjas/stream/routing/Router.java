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
package org.robotninjas.stream.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

import com.google.common.collect.Range;
import com.google.common.hash.HashFunction;

import static com.google.common.base.Charsets.UTF_8;

public class Router<T extends Node> {

  private final NavigableMap<Long, T> ring;
  private final HashFunction hashFunction;

  public Router(HashFunction hashFunction) {
    this.ring = new ConcurrentSkipListMap<>();
    this.hashFunction = hashFunction;
  }

  private long hashed(String value) {
    return hashFunction.hashString(value, UTF_8).asLong();
  }

  public T getNode(String id) {
    return ring.ceilingEntry(hashed(id)).getValue();
  }

  public T addNode(T node) {
    ring.put(hashed(node.id()), node);
    return node;
  }

  public Node removeNode(String id) {
    return ring.remove(hashed(id));
  }

  public void removeNodes(Predicate<T> pred) {
    ring.values().removeIf(pred);
  }

  public Map<T, Range<Long>> nodeRangeMap() {
    var map = new HashMap<T, Range<Long>>();
    Map.Entry<Long, T> previous = ring.lastEntry();
    for (var entry : ring.entrySet()) {
      map.put(
        entry.getValue(),
        Range.closedOpen(previous.getKey(), entry.getKey())
      );
      previous = entry;
    }
    return map;
  }
}
