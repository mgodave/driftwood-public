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
package org.robotninjas.stream.filters;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.SubscriberName;
import reactor.core.publisher.Flux;

public interface MetricsFilter {
  String SubscriberTag = "SubscriberName";
  String StreamNameTag = "StreamName";
  String OperationTag = "Operation";

  static <T> Function<Publisher<T>, Publisher<T>> op(Tags tags) {
    return request -> Flux.from(request)
      .tag(StreamNameTag, tags.stream().name())
      .tag(OperationTag, tags.operation().name())
      .metrics();
  }

  enum Operation {
    Read,
    Write,
    Truncate,
    MarkEnd,
    Push,
    Pull,
    Empty
  }

  record Tags(StreamName stream, Operation operation) {
    static Tags Empty = new Tags(StreamName.Empty, Operation.Empty);

    public static Tags read(StreamName stream) {
      return new Tags(stream, Operation.Write);
    }

    public static Tags write(StreamName stream) {
      return new Tags(stream, Operation.Write);
    }

    public static Tags markEnd(StreamName stream) {
      return new Tags(stream, Operation.MarkEnd);
    }

    public static Tags truncate(StreamName stream) {
      return new Tags(stream, Operation.Truncate);
    }

    public static Tags push(StreamName stream) {
      return new Tags(stream, Operation.Push);
    }

    public static Tags pull(StreamName stream) {
      return new Tags(stream, Operation.Pull);
    }

    public static Tags subscribe(SubscriberName subscriber) {
      return Empty; //TODO this is obviously not right
    }
  }
}
