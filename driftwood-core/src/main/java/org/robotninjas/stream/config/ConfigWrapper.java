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
package org.robotninjas.stream.config;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.robotninjas.stream.Config;

public class ConfigWrapper {

  static Function<String, Object> getTransformFunction(Type type) {
    if (type.equals(Integer.class)) {
      return Integer::parseInt;
    } else if (type.equals(Double.class)) {
      return Double::parseDouble;
    } else if (type.equals(Float.class)) {
      return Float::parseFloat;
    } else if (type.equals(Boolean.class)) {
      return Boolean::parseBoolean;
    } else if (type.equals(String.class)) {
      return (String s) -> s;
    }
    return null;
  }

  static Supplier<Object> getFunctionForReturnType(Config config, Method method) {
    var property = propertyFromMethodName(method.getName());
    var genericReturnType = method.getGenericReturnType();
    if (method.getReturnType().equals(Optional.class)) {
      var contains = ((ParameterizedType) genericReturnType).getActualTypeArguments()[0];
      return () -> config.get(property).map(getTransformFunction(contains));
    } else if (method.getReturnType().equals(List.class)) {
      var contains = ((ParameterizedType) genericReturnType).getActualTypeArguments()[0];
      return () -> {
        var propValue = config.get(property).orElse("");
        return Stream.of(propValue.split(",")).map(getTransformFunction(contains)).collect(Collectors.toList());
      };
    }
    return () -> config.get(property).map(getTransformFunction(method.getReturnType())).get();
  }

  @SuppressWarnings("unchecked")
  public static <T> T proxy(Class<T> proxy, Config config) {
    LoadingCache<Method, Supplier<Object>> cachedInvocations =
      Caffeine.newBuilder().build(new CacheLoader<>() {
        @Override
        public @Nullable Supplier<Object> load(@NonNull Method method)  {
          return getFunctionForReturnType(config, method);
        }
      });

    return (T) Proxy.newProxyInstance(Config.class.getClassLoader(), new Class[]{proxy}, (proxy1, method, args) -> {
      Preconditions.checkState(args == null || args.length == 0);
      return Objects.requireNonNull(cachedInvocations.get(method)).get();
    });
  }

  static String propertyFromMethodName(String methodName) {
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, methodName).replace('_', '.');
  }

}
