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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.robotninjas.stream.security.AuthToken;
import picocli.CommandLine.Option;

public class AuthOptions {
  private String user;
  private char[] password;
  private File passwordFile;

  public AuthOptions() {
  }

  public AuthOptions(String user, char[] password) {
    this.user = user;
    this.password = password;
  }

  public String user() {
    return user;
  }

  @Option(names = "--user")
  public AuthOptions user(String user) {
    this.user = user;
    return this;
  }

  public char[] password() {
    return password;
  }

  @Option(names = "--password", arity = "0..1", interactive = true)
  public AuthOptions password(char[] password) {
    this.password = password;
    return this;
  }

  public File passwodFile() {
    return passwordFile;
  }
  
  @Option(names = "--password:file")
  public AuthOptions passwordFile(File passwordFile) {
    this.passwordFile = passwordFile;
    return this;
  }

  public AuthToken token() {
    try {
      var pw = password != null
        ? new String(password)
        : Files.readString(passwordFile.toPath());
      return AuthToken.fromUsernameAndPassword(user, pw);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
