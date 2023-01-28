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
package org.robotninjas.stream.security;

import org.robotninjas.stream.StreamName;
import org.robotninjas.stream.SubscriberName;

public final class Permissions {
  private Permissions() {}

  public static Permission read(StreamName stream) {
    return new Stream.Perms(Stream.Op.Read, stream);
  }

  public static Permission write(StreamName stream) {
    return new Stream.Perms(Stream.Op.Write, stream);
  }

  public static Permission truncate(StreamName stream) {
    return new Stream.Perms(Stream.Op.Truncate, stream);
  }

  public static Permission end(StreamName stream) {
    return new Stream.Perms(Stream.Op.End, stream);
  }

  public static Permission push(StreamName stream) {
    return new Stream.Perms(Stream.Op.Push, stream);
  }

  public static Permission pull(StreamName stream) {
    return new Stream.Perms(Stream.Op.Pull, stream);
  }

  public static Permission all(StreamName stream) {
    return new Stream.Perms(Stream.Op.All, stream);
  }

  public static Permission publish(SubscriberName subscriber) {
    return new Subscriber.Perms(Subscriber.Op.Publish, subscriber);
  }

  public static Permission subscribe(SubscriberName subscriber) {
    return new Subscriber.Perms(Subscriber.Op.Subscribe, subscriber);
  }

  public static Permission all(SubscriberName subscriber) {
    return new Subscriber.Perms(Subscriber.Op.All, subscriber);
  }

  public enum Resource {
    Stream("stream"),
    Subscriber("subscriber");

    private final String str;

    Resource(String str) {
      this.str = str;
    }

    @Override
    public String toString() {
      return str;
    }
  }

  public sealed interface Op {}

  public final static class Stream {
    private Stream() {}

    enum Op implements Permissions.Op {
      Read("read"),
      Write("write"),
      Truncate("truncate"),
      End("end"),
      All("*"),
      Push("push"),
      Pull("pull"),
      None("NONE");

      private final String str;

      Op(String str) {
        this.str = str;
      }

      @Override
      public String toString() {
        return str;
      }
    }

    record Perms(Resource resource, Stream.Op operation, String instance) implements Permission {
      public Perms(Stream.Op operation, StreamName stream) {
        this(Resource.Subscriber, operation, stream.name());
      }
    }
  }

  public final static class Subscriber {
    private Subscriber() {}

    enum Op implements Permissions.Op {
      Subscribe("subscribe"),
      Publish("publsih"),
      All("*"),
      None("NONE");

      private final String str;

      Op(String str) {
        this.str = str;
      }

      @Override
      public String toString() {
        return str;
      }
    }

    record Perms(Resource resource, Subscriber.Op operation, String instance) implements Permission {
      public Perms(Subscriber.Op operation, SubscriberName subscriber) {
        this(Resource.Subscriber, operation, subscriber.name());
      }
    }
  }

}
