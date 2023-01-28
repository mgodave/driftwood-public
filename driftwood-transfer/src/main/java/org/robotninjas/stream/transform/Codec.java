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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema;
import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

public interface Codec<T> {
  static ProtobufCodec protobuf(ProtobufSchema schema) {
    return new ProtobufCodec(schema);
  }

  static JsonCodec json() {
    return new JsonCodec();
  }

  static AvroCodec avro(AvroSchema schema) {
    return new AvroCodec(schema);
  }

  static CsvCodec csv(CsvSchema schema) {
    return new CsvCodec(schema);
  }

  static JavaPropsCodec javaProps(JavaPropsSchema schema) {
    return new JavaPropsCodec(schema);
  }

  static TomlCodec toml() {
    return new TomlCodec();
  }

  static YamlCodec yaml() {
    return new YamlCodec();
  }

  static XmlCodec xml() {
    return new XmlCodec();
  }

  T deserialize(ByteBuffer bytes) throws IOException;

  ByteBuffer serialize(T object) throws IOException;

  interface JacksonCodec extends Codec<JsonNode> {
    ObjectMapper mapper();

    @Override
    default JsonNode deserialize(ByteBuffer bytes) throws IOException {
      return mapper().reader().readTree(new ByteBufferBackedInputStream(bytes));
    }

    @Override
    default ByteBuffer serialize(JsonNode tree) throws IOException {
      return ByteBuffer.wrap(mapper().writeValueAsBytes(tree));
    }
  }

  interface JacksonSchemaCodec extends JacksonCodec {
    FormatSchema schema();

    @Override
    default JsonNode deserialize(ByteBuffer bytes) throws IOException {
      return mapper().reader(schema()).readTree(new ByteBufferBackedInputStream(bytes));
    }

    @Override
    default ByteBuffer serialize(JsonNode tree) throws IOException {
      return ByteBuffer.wrap(mapper().writer().with(schema()).writeValueAsBytes(tree));
    }
  }

  record ProtobufCodec(ProtobufMapper mapper, ProtobufSchema schema) implements JacksonSchemaCodec {
    private static final ProtobufMapper DefaultMapper = new ProtobufMapper();
    public ProtobufCodec(ProtobufSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record JsonCodec(ObjectMapper mapper) implements JacksonCodec {
    private static final ObjectMapper DefaultMapper = new ObjectMapper();
    public JsonCodec() {
      this(DefaultMapper);
    }
  }

  record AvroCodec(AvroMapper mapper, AvroSchema schema) implements JacksonSchemaCodec {
    private static final AvroMapper DefaultMapper = new AvroMapper();
    public AvroCodec(AvroSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record CsvCodec(CsvMapper mapper, CsvSchema schema) implements JacksonSchemaCodec {
    private static final CsvMapper DefaultMapper = new CsvMapper();
    public CsvCodec(CsvSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record JavaPropsCodec(JavaPropsMapper mapper, JavaPropsSchema schema) implements JacksonSchemaCodec {
    private static final JavaPropsMapper DefaultMapper = new JavaPropsMapper();
    public JavaPropsCodec(JavaPropsSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record TomlCodec(TomlMapper mapper) implements JacksonCodec {
    private static final TomlMapper DefaultMapper = new TomlMapper();
    public TomlCodec() {
      this(DefaultMapper);
    }
  }

  record YamlCodec(YAMLMapper mapper) implements JacksonCodec {
    private static final YAMLMapper DefaultMapper = new YAMLMapper();
    public YamlCodec() {
      this(DefaultMapper);
    }
  }

  record XmlCodec(XmlMapper mapper) implements JacksonCodec {
    private static final XmlMapper DefaultMapper = new XmlMapper();
    public XmlCodec() {
      this(DefaultMapper);
    }
  }
}
