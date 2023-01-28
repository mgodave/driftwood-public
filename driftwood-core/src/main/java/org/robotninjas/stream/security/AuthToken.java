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
package org.robotninjas.stream.security;

import java.util.Base64;

import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.BearerToken;
import org.apache.shiro.authc.UsernamePasswordToken;

import static java.nio.charset.StandardCharsets.UTF_8;

public sealed interface AuthToken {
  Base64.Decoder DefaultDecoder = Base64.getDecoder();
  Base64.Encoder DefaultEncoder = Base64.getEncoder();

  AuthToken Null = new NullAuthToken();

  static AuthToken fromString(String header) {
    var schemeAndToken = header.split(" ", -1);
    if (schemeAndToken.length != 2) {
      throw new IllegalArgumentException();
    }

    return switch (schemeAndToken[0].toLowerCase()) {
      case "basic" -> BasicAuthToken.fromString(schemeAndToken[1]);
      case "bearer" -> BearerAuthToken.fromString(schemeAndToken[1]);
      default -> throw new IllegalArgumentException(schemeAndToken[0]);
    };
  }

  static AuthToken fromUsernameAndPassword(String username, String password) {
    return new BasicAuthToken(new UsernamePasswordToken(username, password));
  }

  static AuthToken fromBearerToken(String bearerToken) {
    return new BearerAuthToken(new BearerToken(bearerToken));
  }

  AuthenticationToken token();

  default void clear() { }

  Scheme scheme();

  String payload();

  enum Scheme {
    NONE(""),
    BEARER("Bearer"),
    BASIC("Basic");

    private final String str;

    Scheme(String str) {
      this.str = str;
    }

    @Override
    public String toString() {
      return str;
    }
  }

  record BasicAuthToken(UsernamePasswordToken token) implements AuthToken {
    public static BasicAuthToken fromString(String token) {
      var decoded = new String(DefaultDecoder.decode(token), UTF_8);

      var usernameAndPassword = decoded.split(":", -1);
      if (usernameAndPassword.length != 2) {
        throw new IllegalArgumentException();
      }

      return new BasicAuthToken(new UsernamePasswordToken(
        usernameAndPassword[0],
        usernameAndPassword[1]
      ));
    }

    @Override
    public void clear() {
      token.clear();
    }

    @Override
    public String payload() {
      var str = token.getUsername() + ":" + new String(token.getPassword());
      return DefaultEncoder.encodeToString(str.getBytes(UTF_8));
    }

    @Override
    public Scheme scheme() {
      return Scheme.BASIC;
    }
  }

  record BearerAuthToken(BearerToken token) implements AuthToken {
    public static BearerAuthToken fromString(String token) {
      return new BearerAuthToken(new BearerToken(token));
    }

    @Override
    public String payload() {
      return DefaultEncoder.encodeToString(token.getToken().getBytes(UTF_8));
    }

    @Override
    public Scheme scheme() {
      return Scheme.BEARER;
    }
  }

  final class NullAuthToken implements AuthToken {
    @Override
    public AuthenticationToken token() {
      return null;
    }

    @Override
    public String payload() {
      return "";
    }

    @Override
    public Scheme scheme() {
      return Scheme.NONE;
    }
  }
}
