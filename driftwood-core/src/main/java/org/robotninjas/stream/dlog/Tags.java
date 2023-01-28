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
package org.robotninjas.stream.dlog;

import java.util.Collections;
import java.util.List;

import io.micrometer.core.instrument.Tag;
import reactor.core.Scannable;

public final class Tags {
  private Tags() {}
  public static List<Tag> get(Object o) {
    var scannable = Scannable.from(o);
    if (scannable.isScanAvailable()) {
      return scannable.tags().map(ts ->
        Tag.of(ts.getT1(), ts.getT2())
      ).toList();
    }
    return Collections.emptyList();
  }
}
