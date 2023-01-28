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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.AbstractService;
import org.robotninjas.stream.Record;
import org.robotninjas.stream.Start;
import org.robotninjas.stream.StreamFactory;
import org.robotninjas.stream.StreamManager;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.TxId;
import reactor.core.publisher.Mono;

public class StateMachine extends AbstractService {
  private static final StreamName UpdateStream = StreamName.of("updates");
  private final ConcurrentHashMap<UUID, Consumer<String>> callbacks = new ConcurrentHashMap<>();
  private final StreamManager streamManager;
  private ConcurrentHashMap<String, String> state = new ConcurrentHashMap<>();

  public StateMachine(StreamFactory streamFactory) {
    this.streamManager = streamFactory.get(UpdateStream);
  }

  public Optional<String> get(String key) {
    return Optional.ofNullable(state.get(key));
  }

  private Mono<String> internalWriteOp(Op operation) {
    var completable = new CompletableFuture<String>();
    callbacks.put(operation.id(), completable::complete);
    try {
      var record = Record.create(ByteBuffer.allocate(0), TxId.Empty);
      return streamManager.write(record).flatMap(ignore ->
        Mono.fromCompletionStage(completable)
      );
    } catch (RuntimeException e) {
      callbacks.remove(operation.id());
      throw e;
    }
  }

  public Mono<String> set(String key, String value) {
    return internalWriteOp(new Op.SetOp(UUID.randomUUID(), key, value));
  }

  public Mono<String> checkAndSet(String key, String old, String value) {
    return internalWriteOp(new Op.CheckAndSetOp(UUID.randomUUID(), key, old, value));
  }

  private Entry entry(Record.Stored stored) {
    return null;
  }

  @Override
  protected void doStart() {
    streamManager.read(Start.Oldest).map(this::entry).doOnNext( entry -> {
      if (entry instanceof Entry.Checkpoint checkpoint) {
        this.state = new ConcurrentHashMap<>(checkpoint.state);
      } else if (entry instanceof Entry.OpEntry op) {
        String value = "";
        if (op.operation() instanceof Op.SetOp set) {
          state.put(set.key(), set.value());
          value = set.value();
        } else if (op.operation() instanceof Op.CheckAndSetOp checkAndSet) {
          if (state.get(checkAndSet.key).equals(checkAndSet.prev())) {
            state.put(checkAndSet.key(), checkAndSet.next());
            value = checkAndSet.next();
          } else {
            value = checkAndSet.prev();
          }
        }

        callbacks.getOrDefault(op.operation().id(), (String ignore) ->  {}).accept(value);
      }
    });
  }

  @Override
  protected void doStop() {

  }

  sealed interface Op {
    UUID id();
    record SetOp(UUID id, String key, String value) implements Op {}
    record CheckAndSetOp(UUID id, String key, String prev, String next) implements Op {}
  }

  sealed interface Entry {
    record OpEntry(Op operation) implements Entry {}
    record Checkpoint(Map<String, String> state) implements Entry {}
  }

}
