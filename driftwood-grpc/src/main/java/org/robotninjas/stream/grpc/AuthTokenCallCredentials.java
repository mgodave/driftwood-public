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
package org.robotninjas.stream.grpc;

import java.util.concurrent.Executor;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import org.robotninjas.stream.security.AuthToken;

import static org.robotninjas.stream.grpc.AuthServerInterceptor.AuthMetadataKey;

public class AuthTokenCallCredentials extends CallCredentials {
  private final AuthToken token;

  public AuthTokenCallCredentials(AuthToken token) {
    this.token = token;
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    Metadata metadata = new Metadata();
    var encoded = token.scheme().toString() + " " + token.payload();
    metadata.put(AuthMetadataKey, encoded);
    applier.apply(metadata);
  }

  @Override
  public void thisUsesUnstableApi() { }
}
