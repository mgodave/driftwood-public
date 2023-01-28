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
package org.robotninjas.stream.transform;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.Versions;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface Transform<I, O> extends Function<I, O> {

  static <I> Transform<I, I> identity() {
    return in -> in;
  }

  static Transform<JsonNode, Mono<JsonNode>> query(String query) {
    return JqTransform.create(query, DefaultCompiledQueryCacheHolder.INSTANCE);
  }

  static Transform<JsonNode, Mono<JsonNode>> query(String query, Cache<String, JsonQuery> cache) {
    return JqTransform.create(query, cache);
  }

  static Transform<JsonNode, Mono<JsonNode>> path(String path) {
    return new JPathTransform(path, DefaultCompiledPathCacheHolder.INSTANCE);
  }

  static Transform<JsonNode, Mono<JsonNode>> path(String path, Cache<String, JsonPath> cache) {
    return new JPathTransform(path, cache);
  }

  static <I, O> Transform<ByteBuffer, Mono<ByteBuffer>> adapt(Function<I, O> fn, Codec<I> codecIn, Codec<O> codecOut) {
    return (ByteBuffer input) -> Mono.fromCallable(() -> codecOut.serialize(fn.apply(codecIn.deserialize(input))));
  }

  static <O> Transform<ByteBuffer, Mono<O>> deserialize(Codec<O> codec) {
    return (ByteBuffer input) -> Mono.fromCallable(() -> codec.deserialize(input));
  }

  static <I> Transform<I, Mono<ByteBuffer>> serialize(Codec<I> codec) {
    return (I input) -> Mono.fromCallable(() -> codec.serialize(input));
  }

  static <I> Transform<I, Mono<I>> filter(Predicate<I> pred) {
    return (I input) -> Mono.just(input).filter(pred);
  }

  static Function<JsonNode, JsonNode> wrapResults(ObjectMapper mapper, String name) {
    return (JsonNode list) -> {
      var objectNode = mapper.createObjectNode();
      objectNode.set(name, list);
      return objectNode;
    };
  }

  class DefaultCompiledPathCacheHolder {
    private static final Cache<String, JsonPath> INSTANCE = Caffeine.newBuilder()
      .maximumSize(100).expireAfterAccess(Duration.ofMinutes(10)).build();
  }

  class DefaultCompiledQueryCacheHolder {
    private static final Cache<String, JsonQuery> INSTANCE = Caffeine.newBuilder()
      .maximumSize(100).expireAfterAccess(Duration.ofMinutes(10)).build();
  }

  record JPathTransform(JsonPath path) implements Transform<JsonNode, Mono<JsonNode>> {
    public static final ObjectMapper DefaultMapper = new ObjectMapper();
    private static final Configuration DefaultConfig = Configuration.builder()
      .jsonProvider(new JacksonJsonNodeJsonProvider(DefaultMapper))
      .mappingProvider(new JacksonMappingProvider(DefaultMapper))
      .options(Option.ALWAYS_RETURN_LIST)
      .build();

    public JPathTransform(String path, Cache<String, JsonPath> cache) {
      this(cache.get(path, JsonPath::compile));
    }

    @Override
    public Mono<JsonNode> apply(JsonNode tree) {
      return Mono.just(path.read(tree, DefaultConfig));
    }
  }

  record JqTransform(Scope scope, JsonQuery query) implements Transform<JsonNode, Mono<JsonNode>> {
    public static final Scope DefaultRootScope = Scope.newEmptyScope();
    public static final Version DefaultVersion = Versions.JQ_1_6;

    static {
      BuiltinFunctionLoader.getInstance()
        .loadFunctions(DefaultVersion, DefaultRootScope);
    }

    static JqTransform create(String queryString, Cache<String, JsonQuery> cache) {
      var scope = Scope.newChildScope(DefaultRootScope);
      JsonQuery query = cache.get(queryString, q -> {
        try {
          return JsonQuery.compile(q, DefaultVersion);
        } catch (JsonQueryException e) {
          //TODO DO NOT THROW!!!!
          throw new RuntimeException(e);
        }
      });
      return new JqTransform(scope, query);
    }

    @Override
    public Mono<JsonNode> apply(JsonNode tree) {
      return Mono.create(sink -> {
        try {
          query.apply(scope, tree, sink::success);
        } catch (JsonQueryException e) {
          sink.error(e);
        }
      });
    }

    public JqTransform setValue(String name, JsonNode value) {
      this.scope.setValue(name, value);
      return this;
    }
  }

  record JsonSurferTransform(String path) implements Transform<InputStream, Mono<String>> {
    private static final JsonSurfer Surfer = JsonSurferJackson.INSTANCE;

    @Override
    public Mono<String> apply(InputStream input) {
      return Mono.create(sink -> {
        @SuppressWarnings("unchecked")
        var configuration = Surfer.configBuilder()
          .bind(path, String.class,
            (value, context) -> sink.success(value)
          ).build();
        Surfer.surf(input, configuration);
      });
    }
  }
}
