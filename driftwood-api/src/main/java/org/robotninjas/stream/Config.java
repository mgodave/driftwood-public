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

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

@SuppressWarnings("unchecked")
public interface Config {
  Optional<String> get(String s);

  static Config fromProperties(Properties properties) {
    return new PropertiesConfig(properties);
  }

  static Config fromMap(Map<String, String> map) {
    return new MapConfig(map);
  }

  record MapConfig(Map<String, String> map) implements Config {
    @Override
    public Optional<String> get(String s) {
      return Optional.ofNullable(map.get(s));
    }
  }

  record PropertiesConfig(Properties properties) implements Config {
    @Override
    public Optional<String> get(String s) {
      return Optional.ofNullable(properties.getProperty(s));
    }
  }
}
