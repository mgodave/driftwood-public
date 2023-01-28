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
package org.robotninjas.stream.examples;

import java.util.function.Function;

import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.NetworkBuilder;
import org.robotninjas.stream.StreamName;

@SuppressWarnings("UnusedVariable")
public class DagExample {
  interface Node {
    String name();
  }

  record MapNode<I, O>(Function<I, O> fn) implements Node {
    @Override
    public String name() {
      return "mapnode";
    }
  }

  private static StreamName stream(String name) {
    return StreamName.of(name);
  }

  private static Node node(String name) {
    return () -> name;
  }

  public static void main(String[] args) {

    ImmutableNetwork<Node, StreamName> network =
      NetworkBuilder.directed().<Node, StreamName>immutable()
        .addEdge(node("INPUT"), node("MAP1"), stream("INPUT_STREAM"))
        .addEdge(node("MAP1"), node("FILTER1"), stream("MAP1_STREAM"))
        .addEdge(node("FILTER1"), node("PARTITION1"), stream("FILTER1_STREAM"))
        .addEdge(node("PARTITION1"), node("MAP2_1"), stream("PARTITION1_1_STREAM"))
        .addEdge(node("PARTITION1"), node("MAP2_2"), stream("PARTITION1_2_STREAM"))
        .addEdge(node("PARTITION1"), node("MAP2_3"), stream("PARTITION1_3_STREAM"))
        .addEdge(node("MAP2_1"), node("COLLECT1"), stream("MAP2_1_STREAM"))
        .addEdge(node("MAP2_2"), node("COLLECT1"), stream("MAP2_2_STREAM"))
        .addEdge(node("MAP2_3"), node("COLLECT1"), stream("MAP2_3_STREAM"))
        .addEdge(node("COLLECT1"), node("OUTPUT"), stream("OUTPUT_STREAM"))
        .build();

  }
}
