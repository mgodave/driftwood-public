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
package org.robotninjas.app.stream;

import org.robotninjas.app.App;
import org.robotninjas.app.stream.commands.StreamReadProxyCommand;
import org.robotninjas.app.stream.commands.StreamReaderCommand;
import org.robotninjas.app.stream.commands.StreamStandaloneCommand;
import org.robotninjas.app.stream.commands.StreamWriteProxyCommand;
import org.robotninjas.app.stream.commands.StreamWriterCommand;
import picocli.CommandLine;

@CommandLine.Command(
   name = "stream",
   subcommands = {
      StreamReadProxyCommand.class,
      StreamReaderCommand.class,
      StreamWriteProxyCommand.class,
      StreamWriterCommand.class,
      StreamStandaloneCommand.class
   },
   abbreviateSynopsis = true
)
public class Stream implements Runnable {
   public static void main(String[] args) {
      App.execute(Stream.class, args);
   }

   @Override
   public void run() {}
}
