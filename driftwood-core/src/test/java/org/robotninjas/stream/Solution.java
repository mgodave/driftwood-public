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
package org.robotninjas.stream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Solution {
  interface Datasource<T> {
    Stream<T> collect();

    default <U> Datasource<U> map(Function<T, U> f) {
      return new MapOp<>(this, f);
    }

    default Datasource<T> filter(Predicate<T> p) {
      return new FilterOp<>(this, p);
    }

    default <U> Datasource<U> flatMap(Function<T, Datasource<U>> f) {
      return new FlatMapOp<>(this, f);
    }

    default Datasource<T> take(int n) {
      return new TakeOp<>(this, n);
    }

    default Datasource<T> concat(Datasource<T> datasource) {
      return new ConcatOp<>(this, datasource);
    }

    record ConcatOp<T>(Datasource<T> data1, Datasource<T> data2) implements Datasource<T> {
      @Override public Stream<T> collect() {
        return Stream.concat(data1.collect(), data2.collect());
      }
    }

    record TakeOp<T>(Datasource<T> data, int n) implements Datasource<T> {
      @Override public Stream<T> collect() {
        return data.collect().limit(n);
      }
    }

    record SimpleDatasource<T>(List<T> data) implements Datasource<T> {
      @Override public Stream<T> collect() {
        return data().stream();
      }
    }

    record OnDemandDatasource<T>(Supplier<T> supplier) implements Datasource<T> {
      @Override public Stream<T> collect() {
        return Stream.generate(supplier());
      }
    }

    record MapOp<T, U>(Datasource<T> source, Function<T, U> f) implements Datasource<U> {
      @Override public Stream<U> collect() {
        return source.collect().map(f);
      }
    }

    record FilterOp<T>(Datasource<T> source, Predicate<T> p) implements Datasource<T> {
      @Override public Stream<T> collect() {
        return source.collect().filter(p);
      }
    }

    record FlatMapOp<T, U>(Datasource<T> source, Function<T, Datasource<U>> f) implements Datasource<U> {
      @Override public Stream<U> collect() {
        return source.collect().flatMap(e -> f.apply(e).collect());
      }
    }

    static <T> Datasource<T> of(List<T> data) {
      return new SimpleDatasource<>(data);
    }
    
    @SafeVarargs
    static <T> Datasource<T> of (T... data) {
      return new SimpleDatasource<>(List.of(data));
    }

    static <T> Datasource<T> from(Supplier<T> supplier) {
      return new OnDemandDatasource<>(supplier);
    }

    static <T> List<T> eval(Datasource<T> data) {
      return data.collect().toList();
    }

    static <T> Datasource<T> concat(Datasource<T> data1, Datasource<T> data2) {
      return new ConcatOp<>(data1, data2);
    }

    Datasource<?> Empty = Datasource.of();
  }

  public @Test
  void testSimply() {
    Datasource<Integer> initial = Datasource.of(1, 2, 3);
    Assert.assertEquals(List.of(1, 2, 3), Datasource.eval(initial));

    Datasource<Integer> first = initial.map(x -> x * 2);
    Assert.assertEquals(List.of(2, 4, 6), Datasource.eval(first));

    Datasource<Integer> second = initial.filter(x -> x % 2 == 0);
    Assert.assertEquals(List.of(2), Datasource.eval(second));

    Datasource<String> third = initial.map(Object::toString);
    Assert.assertEquals(List.of("1", "2", "3"), Datasource.eval(third));

    Datasource<Integer> fourth = initial.flatMap(x -> {
      var array = new Integer[x];
      Arrays.fill(array, x);
      return Datasource.of(array);
    });
    Assert.assertEquals(List.of(1, 2, 2, 3, 3, 3), Datasource.eval(fourth));

    Datasource<Integer> fifth = initial
      .map(x -> x * 2)
      .filter(x -> x % 3 == 0)
      .flatMap(x -> Datasource.of(x * 3).map(y -> y + 2));
    Assert.assertEquals(List.of(20), Datasource.eval(fifth));

    AtomicInteger i = new AtomicInteger(0);
    Datasource<Integer> sixth = Datasource.from(i::getAndIncrement).take(5);
    Assert.assertEquals(List.of(0, 1, 2, 3, 4), Datasource.eval(sixth));

    Datasource<Integer> seventh = Datasource.of(1, 2, 3)
      .concat(Datasource.of(4, 5, 6));
    Assert.assertEquals(List.of(1, 2, 3, 4, 5, 6), Datasource.eval(seventh));

    Datasource<Integer> eigth = Datasource.concat(Datasource.of(1, 2, 3), Datasource.of(4, 5, 6));
    Assert.assertEquals(List.of(1, 2, 3, 4, 5, 6), Datasource.eval(eigth));
  }
  
  static void main(String[] args) {
    JUnitCore.main("Solution");
  }
}