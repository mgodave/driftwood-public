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

import java.time.Duration;
import java.util.Random;
import java.util.function.Function;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.Subject;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface Security {
  Duration DefaultRefresh = Duration.ofSeconds(10);

  Mono<Principal> login(CallContext ctx);

  // likely only usable for testing or scenarios
  // where auth is handled solely by mTLS
  Security Open = ctx -> Mono.just(Principal.Open);

  static Security closed(Throwable t) {
    return ctx -> Mono.just(Principal.closed(t));
  }

  record ShiroSecurity(SecurityManager mgr) implements Security {
    public Mono<Principal> login(CallContext ctx) {
      return Mono.fromCallable(() -> {
        var subject = new Subject.Builder(mgr).buildSubject();
        subject.login(ctx.authToken().token());
        // clear the token so auth is no longer present in memory
        ctx.authToken().clear();
        return new ShiroPrincipal(subject);
      });
    }
  }

  record ShiroPrincipal(Subject subject) implements Principal {
    @Override
    public Mono<Principal> check(Permission perms) {
      return Mono.fromCallable(() -> {
        subject.checkPermission(new WildcardPermission(
          String.join(":",
            perms.resource().toString(),
            perms.operation().toString(),
            perms.instance()
          )
        ));
        return this;
      });
    }
  }


  record Op<T>(Principal principal, Permission permissions) implements Function<Publisher<T>, Publisher<T>> {
    @Override
    public Publisher<T> apply(Publisher<T> tPublisher) {
      //TODO we should generate some jitter here otherwise we run the risk of this syncing up globally.
      var permissionsRefresh = Flux.interval(Duration.ZERO, DefaultRefresh).flatMap(ignore ->
        principal.check(permissions)
      );

      return permissionsRefresh.switchOnFirst((first, checks) ->
        // force the first check to complete before processing any events.
        Flux.from(tPublisher).withLatestFrom(checks, (request, ignore) -> request)
      );
    }
  }

  static <T> Function<Publisher<T>, Publisher<T>> op(Principal principal, Permission permissions) {
    return new Op<>(principal, permissions);
  }
}
