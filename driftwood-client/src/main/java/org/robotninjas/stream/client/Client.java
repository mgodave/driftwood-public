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
package org.robotninjas.stream.client;

import java.io.Closeable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

import org.robotninjas.stream.proto.Read;
import org.robotninjas.stream.proto.Write;
import reactor.core.publisher.Flux;

public final class Client {
  private static final Map<String, Reader> READERS = new HashMap<>();
  private static final Map<String, Writer> WRITERS = new HashMap<>();

  static {
    var readLoader = ServiceLoader.load(Reader.class);
    for (Reader f : readLoader) {
      READERS.put(f.protocol(), f);
    }

    var writeLoader = ServiceLoader.load(Writer.class);
    for (Writer f : writeLoader) {
      WRITERS.put(f.protocol(), f);
    }
  }

  private Client() {
  }

  public static StreamReader reader(URI uri) {
    return StreamReader.from(READERS.get(uri.getScheme()));
  }

  public static StreamWriter writer(URI uri) {
    return StreamWriter.from(WRITERS.get(uri.getScheme()));
  }

  public interface Reader extends Function<Read.Request, Flux<Read.Response>>, Closeable {
    String protocol();
  }

  public interface Writer extends Function<Flux<Write.Request>, Flux<Write.Response>>, Closeable {
    String protocol();
  }
}
