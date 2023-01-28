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
package org.robotninjas.stream.admin;

import java.sql.SQLException;

import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.field.types.BaseDataType;
import com.j256.ormlite.support.DatabaseResults;
import org.robotninjas.stream.SubscriberName;

class SubscriberNamePersister extends BaseDataType {
  public SubscriberNamePersister() {
    super(SqlType.STRING);
  }

  @Override
  public Object parseDefaultString(FieldType fieldType, String defaultStr) throws SQLException {
    return SubscriberName.of(defaultStr);
  }

  @Override
  public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {
    return results.getString(columnPos);
  }

  @Override
  public Object sqlArgToJava(FieldType fieldType, Object sqlArg, int columnPos) throws SQLException {
    if (sqlArg instanceof String name) {
      return SubscriberName.of(name);
    }

    throw new SQLException(new IllegalArgumentException("Expecting String but found " + sqlArg.getClass().getName()));  }
}
