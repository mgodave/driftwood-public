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

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchemaLoader;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.avro.Schema;
import org.junit.Test;
import org.robotninjas.stream.proto.Bicycle;
import org.robotninjas.stream.proto.Book;
import org.robotninjas.stream.proto.Bookstore;
import org.robotninjas.stream.proto.Store;
import reactor.core.publisher.Flux;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.robotninjas.stream.transform.Codec.avro;
import static org.robotninjas.stream.transform.Codec.json;
import static org.robotninjas.stream.transform.Codec.protobuf;
import static org.robotninjas.stream.transform.Codec.yaml;
import static org.robotninjas.stream.transform.Transform.deserialize;
import static org.robotninjas.stream.transform.Transform.path;
import static org.robotninjas.stream.transform.Transform.query;
import static org.robotninjas.stream.transform.Transform.serialize;
import static org.robotninjas.stream.transform.Transform.wrapResults;

public class TransformTest {

  private final ObjectMapper DefaultJsonMapper = new ObjectMapper();
  private final AvroMapper DefaultAvroMapper = new AvroMapper();
  private final YAMLMapper DefaultYamlMapper = new YAMLMapper();
  private final ProtobufMapper DefaultProtobufMapper = new ProtobufMapper();

  private static final String ExpectedAvro = """
    { "books": [
      {
         "category":"fiction",
         "author":"Evelyn Waugh",
         "title":"Sword of Honour",
         "price":12.99,
         "isbn": null
      },
      {
         "category":"fiction",
         "author":"J. R. R. Tolkien",
         "title":"The Lord of the Rings",
         "isbn":"0-395-19395-8",
         "price":22.99
      }
    ]}
    """;

  private static final String ExpectedQueryResults = """
    { "books": [
      {
         "category":"fiction",
         "author":"Evelyn Waugh",
         "title":"Sword of Honour",
         "price":12.99
      },
      {
         "category":"fiction",
         "author":"J. R. R. Tolkien",
         "title":"The Lord of the Rings",
         "isbn":"0-395-19395-8",
         "price":22.99
      }
    ]}
    """;

  private static Bookstore mkBookstore() {
    return Bookstore.newBuilder()
      .setStore(
        Store.newBuilder()
          .addAllBook(List.of(
            Book.newBuilder()
              .setCategory("reference")
              .setAuthor("Nigel Rees")
              .setTitle("Sayings of the Century")
              .setPrice(8.95)
              .build(),
            Book.newBuilder()
              .setCategory("fiction")
              .setAuthor("Evelyn Waugh")
              .setTitle("Sword of Honour")
              .setPrice(12.99)
              .build(),
            Book.newBuilder()
              .setCategory("fiction")
              .setAuthor("Herman Melville")
              .setTitle("Moby Dick")
              .setIsbn("0-553-21311-3")
              .setPrice(8.99)
              .build(),
            Book.newBuilder()
              .setCategory("fiction")
              .setAuthor("J. R. R. Tolkien")
              .setTitle("The Lord of the Rings")
              .setIsbn("0-395-19395-8")
              .setPrice(22.99)
              .build()
          ))
          .setBicycle(
            Bicycle.newBuilder()
              .setColor("red")
              .setPrice(19.95)
          )
      )
      .setExpensive(10)
      .build();
  }

  @Test
  public void protobufToJsonWithQuery() throws Exception {
    final var jsonCodec = new Codec.Json(new ObjectMapper());

    URL schemaFile = getClass().getClassLoader().getResource("Test.proto");
    ProtobufSchema schema = ProtobufSchemaLoader.std.load(schemaFile);
    final var protobufCodec = new Codec.Protobuf(new ProtobufMapper(), schema);

    Flux<ByteBuffer> records = Flux.just(mkBookstore().toByteString().asReadOnlyByteBuffer());

    var result = records.flatMap(buffer ->
      deserialize(protobufCodec)
        .andThen(m -> m.flatMap(query("{books: [.store.book[] | select(.price > 10)]}")))
        .andThen(m -> m.flatMap(serialize(jsonCodec)))
        .apply(buffer)
    ).blockFirst();

    try(var inputStream = new ByteBufferBackedInputStream(result)) {
      assertEquals(
        DefaultJsonMapper.readTree(ExpectedQueryResults),
        DefaultJsonMapper.readTree(inputStream)
      );
    }
  }

  @Test
  public void protobufToJsonWithPath() throws Exception {
    URL schemaFile = getClass().getClassLoader().getResource("Test.proto");
    ProtobufSchema schema = ProtobufSchemaLoader.std.load(schemaFile);
    final var protobufCodec = protobuf(schema);

    Flux<ByteBuffer> records = Flux.just(mkBookstore().toByteString().asReadOnlyByteBuffer());

    var result = records.flatMap(buffer ->
      deserialize(protobufCodec)
        .andThen(m -> m.flatMap(path("$.store.book[?(@.price > 10)]")))
        .andThen(m -> m.map(wrapResults(protobufCodec.mapper(), "books")))
        .andThen(m -> m.flatMap(serialize(json())))
        .apply(buffer)
    ).blockFirst();

    try(var inputStream = new ByteBufferBackedInputStream(result)) {
      assertEquals(
        DefaultJsonMapper.readTree(ExpectedQueryResults),
        DefaultJsonMapper.readTree(inputStream)
      );
    }
  }

  @Test
  public void protobufToAvroWithQuery() throws Exception {
    URL avroSchemaFile = getClass().getClassLoader().getResource("books.avsc");
    AvroSchema avroSchema = new AvroSchema((new Schema.Parser())
      .parse(requireNonNull(avroSchemaFile).openStream()));

    URL schemaFile = getClass().getClassLoader().getResource("Test.proto");
    ProtobufSchema schema = ProtobufSchemaLoader.std.load(schemaFile);

    Flux<ByteBuffer> records = Flux.just(mkBookstore().toByteString().asReadOnlyByteBuffer());

    var result = records.flatMap(buffer ->
      deserialize(protobuf(schema))
        .andThen(m -> m.flatMap(query("{books: [.store.book[] | select(.price > 10)]}")))
        .andThen(m -> m.flatMap(serialize(avro(avroSchema))))
        .apply(buffer)
    ).blockFirst();

    try(var inputStream = new ByteBufferBackedInputStream(result)) {
      assertEquals(
        DefaultJsonMapper.readTree(ExpectedAvro),
        DefaultAvroMapper.reader(avroSchema).readTree(inputStream)
      );
    }
  }

  @Test
  public void protobufToYamlWithQuery() throws Exception {
    URL schemaFile = getClass().getClassLoader().getResource("Test.proto");
    ProtobufSchema schema = ProtobufSchemaLoader.std.load(schemaFile);

    Flux<ByteBuffer> records = Flux.just(mkBookstore().toByteString().asReadOnlyByteBuffer());

    var result = records.flatMap(buffer ->
      deserialize(protobuf(schema))
        .andThen(m -> m.flatMap(query("{books: [.store.book[] | select(.price > 10)]}")))
        .andThen(m -> m.flatMap(serialize(yaml())))
        .apply(buffer)
    ).blockFirst();

    try(var inputStream = new ByteBufferBackedInputStream(result)) {
      assertEquals(
        DefaultJsonMapper.readTree(ExpectedQueryResults),
        DefaultYamlMapper.reader().readTree(inputStream)
      );
    }
  }

  @Test
  public void protobufToProtobufWithQuery() throws Exception {
    URL outputSchemaFile = getClass().getClassLoader().getResource("books.proto");
    ProtobufSchema outputSchema = ProtobufSchemaLoader.std.load(outputSchemaFile);

    URL schemaFile = getClass().getClassLoader().getResource("Test.proto");
    ProtobufSchema schema = ProtobufSchemaLoader.std.load(schemaFile);

    Flux<ByteBuffer> records = Flux.just(mkBookstore().toByteString().asReadOnlyByteBuffer());

    var result = records.flatMap(buffer ->
      deserialize(protobuf(schema))
        .andThen(m -> m.flatMap(query("{books: [.store.book[] | select(.price > 10)]}")))
        .andThen(m -> m.flatMap(serialize(protobuf(outputSchema))))
        .apply(buffer)
    ).blockFirst();

    try (var inputStream = new ByteBufferBackedInputStream(result)) {
      assertEquals(
        DefaultJsonMapper.readTree(ExpectedQueryResults),
        DefaultProtobufMapper.reader(outputSchema).readTree(inputStream)
      );
    }
  }
}
