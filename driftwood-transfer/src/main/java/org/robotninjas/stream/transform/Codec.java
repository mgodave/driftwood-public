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
  static Protobuf protobuf(ProtobufSchema schema) {
    return new Protobuf(schema);
  }

  static Json json() {
    return new Json();
  }

  static Avro avro(AvroSchema schema) {
    return new Avro(schema);
  }

  static Csv csv(CsvSchema schema) {
    return new Csv(schema);
  }

  static JavaProps javaProps(JavaPropsSchema schema) {
    return new JavaProps(schema);
  }

  static Toml toml() {
    return new Toml();
  }

  static Yaml yaml() {
    return new Yaml();
  }

  static Xml xml() {
    return new Xml();
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

  record Protobuf(ProtobufMapper mapper, ProtobufSchema schema) implements JacksonSchemaCodec {
    private static final ProtobufMapper DefaultMapper = new ProtobufMapper();
    public Protobuf(ProtobufSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record Json(ObjectMapper mapper) implements JacksonCodec {
    private static final ObjectMapper DefaultMapper = new ObjectMapper();
    public Json() {
      this(DefaultMapper);
    }
  }

  record Avro(AvroMapper mapper, AvroSchema schema) implements JacksonSchemaCodec {
    private static final AvroMapper DefaultMapper = new AvroMapper();
    public Avro(AvroSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record Csv(CsvMapper mapper, CsvSchema schema) implements JacksonSchemaCodec {
    private static final CsvMapper DefaultMapper = new CsvMapper();
    public Csv(CsvSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record JavaProps(JavaPropsMapper mapper, JavaPropsSchema schema) implements JacksonSchemaCodec {
    private static final JavaPropsMapper DefaultMapper = new JavaPropsMapper();
    public JavaProps(JavaPropsSchema schema) {
      this(DefaultMapper, schema);
    }
  }

  record Toml(TomlMapper mapper) implements JacksonCodec {
    private static final TomlMapper DefaultMapper = new TomlMapper();
    public Toml() {
      this(DefaultMapper);
    }
  }

  record Yaml(YAMLMapper mapper) implements JacksonCodec {
    private static final YAMLMapper DefaultMapper = new YAMLMapper();
    public Yaml() {
      this(DefaultMapper);
    }
  }

  record Xml(XmlMapper mapper) implements JacksonCodec {
    private static final XmlMapper DefaultMapper = new XmlMapper();
    public Xml() {
      this(DefaultMapper);
    }
  }
}
