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
package org.robotninjas.driftwood.perf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;

public class IterationBenchmark {

  interface Dummy {}

  @State(Scope.Thread)
  public static class BenchmarkState {
    @Param({"1", "10", "100", "1000"})
    int count;

    final List<Dummy> list = new ArrayList<>();

    @Setup
    public void setup() {
      Collections.fill(list, new Dummy() {});
    }

    List<Dummy> doLoop(Blackhole bh) {
      var result = new ArrayList<Dummy>(count);
      for (var dummy : list) {
        bh.consume(dummy);
        result.add(dummy);
      }
      return result;
    }

    List<Dummy> doStream(Blackhole bh) {
      //noinspection SimplifyStreamApiCallChains
      return list.stream().map(dummy -> {
        bh.consume(dummy);
        return dummy;
      }).collect(toList());
    }

    List<Dummy> doFlux(Blackhole h) {
      return Flux.fromIterable(list).map(i -> {
        h.consume(i);
        return i;
      }).collectList().block();
    }
  }

  @Benchmark
  public List<Dummy> loopIteration(BenchmarkState state, Blackhole bh) {
    return state.doLoop(bh);
  }

  @Benchmark
  public List<Dummy> streamIteration(BenchmarkState state, Blackhole bh) {
    return state.doStream(bh);
  }

  @Benchmark
  public List<Dummy> fluxIteration(BenchmarkState state, Blackhole bh) {
    return state.doFlux(bh);
  }

}
