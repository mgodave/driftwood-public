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
package org.robotninjas.stream.server;

import org.checkerframework.dataflow.qual.Pure;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.Start;

import java.util.Objects;

final public class GetStart {
  private GetStart() {}

  @SuppressWarnings({"UnnecessaryParentheses"})
  @Pure public static Start fromProto(org.robotninjas.stream.proto.Start start) {
    Objects.requireNonNull(start);
    return switch (start.getStartPointCase()) {
      case FROMNEWEST -> Start.Newest;
      case FROMOLDEST -> Start.Oldest;
      case FROMOFFSET -> {
        var fromOffset = start.getFromOffset();
        var bytestring = fromOffset.getOffset();
        var bytes = bytestring.toByteArray();
        yield new Start.FromOffset(Offset.from(bytes));
      }
      case STARTPOINT_NOT_SET -> null;
    };
  }
}
