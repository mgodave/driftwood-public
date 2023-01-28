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
package org.robotninjas.stream.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.metadata.CompositeMetadataCodec;
import org.robotninjas.stream.security.AuthToken;

import static io.rsocket.metadata.WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;

public class Metadata {
  static ByteBuf make(AuthToken token, ByteBufAllocator allocator) {
    var metadata = allocator.compositeBuffer();
    CompositeMetadataCodec.encodeAndAddMetadata(
      metadata, allocator, MESSAGE_RSOCKET_AUTHENTICATION, RSocketAuth.encodeMetadata(token, allocator));
    return metadata;
  }

  static ByteBuf make(AuthToken token) {
    return make(token, ByteBufAllocator.DEFAULT);
  }
}
