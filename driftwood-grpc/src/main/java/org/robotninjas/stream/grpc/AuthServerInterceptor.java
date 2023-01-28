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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.robotninjas.stream.security.AuthToken;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.robotninjas.stream.grpc.ContextKeys.AuthTokenContextKey;

public class AuthServerInterceptor implements ServerInterceptor {
  public static final Metadata.Key<String> AuthMetadataKey =
    Metadata.Key.of(AUTHORIZATION, ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
    ServerCall<ReqT, RespT> call,
    Metadata headers,
    ServerCallHandler<ReqT, RespT> next) {

    var authHeader = headers.get(AuthMetadataKey);

    if (authHeader == null) {
      throw new StatusRuntimeException(Status.UNAUTHENTICATED);
    }

    final var auth = AuthToken.fromString(authHeader);
    final var ctx = Context.current().withValue(AuthTokenContextKey, auth);
    return Contexts.interceptCall(ctx, call, headers, next);
  }
}
