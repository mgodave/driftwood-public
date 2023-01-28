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
import io.rsocket.metadata.AuthMetadataCodec;
import org.robotninjas.stream.security.AuthToken;
import org.robotninjas.stream.security.AuthToken.BasicAuthToken;
import org.robotninjas.stream.security.AuthToken.BearerAuthToken;

import static io.rsocket.metadata.WellKnownAuthType.SIMPLE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RSocketAuth {
  static ByteBuf encodeMetadata(AuthToken token, ByteBufAllocator allocator) {
    var metadata = allocator.buffer().writeBytes(token.payload().getBytes(UTF_8));
    return AuthMetadataCodec.encodeMetadata(allocator, SIMPLE, metadata);
  }

  static AuthToken decodeMetadata(ByteBuf metadata) {
    var type = AuthMetadataCodec.readWellKnownAuthType(metadata);
    var content = AuthMetadataCodec.readPayload(metadata);
    return switch (type) {
      case SIMPLE -> BasicAuthToken.fromString(content.toString(UTF_8));
      case BEARER -> BearerAuthToken.fromString(content.toString(UTF_8));
      default -> throw new IllegalArgumentException();
    };
  }
}
