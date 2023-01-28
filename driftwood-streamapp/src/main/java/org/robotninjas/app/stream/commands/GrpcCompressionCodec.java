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
package org.robotninjas.app.stream.commands;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.grpc.Codec;
import org.robotninjas.grpc.codec.Lz4Codec;
import org.robotninjas.grpc.codec.SnappyCodec;

@SuppressWarnings({"unused", "ImmutableEnumChecker"})
public enum GrpcCompressionCodec implements Codec {
  Gzip(new Codec.Gzip()),
  Lz4(new Lz4Codec()),
  Snappy(new SnappyCodec()),
  None(Codec.Identity.NONE);

  private final Codec codec;

  GrpcCompressionCodec(Codec codec) {
    this.codec = codec;
  }

  @Override
  public String getMessageEncoding() {
    return codec.getMessageEncoding();
  }

  @Override
  public InputStream decompress(InputStream is) throws IOException {
    return codec.decompress(is);
  }

  @Override
  public OutputStream compress(OutputStream os) throws IOException {
    return codec.compress(os);
  }
}
