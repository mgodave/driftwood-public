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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.robotninjas.stream.Config;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigWrapperTest {

  @Test
  public void testSimpleInterface() {
    interface MyProperties {
      Integer singleInteger();
      Optional<Integer> optionalInteger();
      List<Integer> listOfIntegers();
    }

    var config = Config.fromMap(Map.of(
      "single.integer", "1",
      "optional.integer", "1",
      "list.of.integers", "1,2,3"
    ));
    var wrapped = ConfigWrapper.proxy(MyProperties.class, config);

    assertEquals(wrapped.singleInteger().intValue(), 1);
    assertEquals(wrapped.optionalInteger(), Optional.of(1));
    assertEquals(wrapped.listOfIntegers(), List.of(1, 2, 3));
  }

}