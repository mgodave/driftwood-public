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

import java.util.stream.Stream;

import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.CompositeMetadata.WellKnownMimeTypeEntry;
import org.robotninjas.stream.security.AuthToken;
import org.robotninjas.stream.security.CallContext;

import static io.rsocket.metadata.WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;

public record RSocketCallContext(AuthToken authToken) implements CallContext {
  public static CallContext fromMetadata(CompositeMetadata metadata) {
    AuthToken token = metadata.stream()
      .filter(entry -> entry instanceof WellKnownMimeTypeEntry wellKnownMimeTypeEntry
        && wellKnownMimeTypeEntry.getType() == MESSAGE_RSOCKET_AUTHENTICATION)
      .map(entry -> RSocketAuth.decodeMetadata(entry.getContent()))
      .findFirst().orElse(AuthToken.Null);
    return new RSocketCallContext(token);
  }
}
