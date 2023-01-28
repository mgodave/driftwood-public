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
package org.robotninjas.stream.transfer;

import java.util.List;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import org.checkerframework.dataflow.qual.Pure;
import org.robotninjas.stream.Offset;
import org.robotninjas.stream.proto.Replicate;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.stream.Collectors.toUnmodifiableList;

@FunctionalInterface
interface ToResponse extends Function<List<? extends Offset>, Replicate.Response> {
  @SuppressWarnings("UnstableApiUsage")
  static List<ByteString> toOffsets(List<? extends Offset> offsets) {
    return offsets.stream().map(offset ->
      unsafeWrap(offset.bytes())
    ).collect(toUnmodifiableList());
  }

  @Pure static ToResponse apply() {
    return (List<? extends Offset> offsets) ->
      Replicate.Response.newBuilder()
        .addAllOffset(toOffsets(offsets))
        .build();
  }
}
