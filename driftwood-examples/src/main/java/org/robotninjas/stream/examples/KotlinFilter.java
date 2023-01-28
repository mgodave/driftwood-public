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
package org.robotninjas.stream.examples;

import javax.script.Invocable;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.util.function.Predicate;

import org.robotninjas.stream.Record;

public class KotlinFilter {

   private final ScriptEngineManager scriptEngineManager;

   public KotlinFilter(ScriptEngineManager scriptEngineManager) {
      this.scriptEngineManager = scriptEngineManager;
   }

   public static KotlinFilter create() {
      return new KotlinFilter(
         new ScriptEngineManager(
            new ScriptClassLoader()
         )
      );
   }

   public Predicate<Record.Stored> load(String fname, String script) throws ScriptException {
      var engine = scriptEngineManager.getEngineByExtension("kts");

      engine.eval(script);

      var invokable = (Invocable) engine;

      return record -> {
         try {
            return (Boolean) invokable.invokeFunction(fname, record);
         } catch (ScriptException | NoSuchMethodException e) {
            // TODO add a metric
            throw new RuntimeException(e);
         }
      };
   }

   // TODO set security parameters and allowed classpath
   // https://stackoverflow.com/questions/502218/sandbox-against-malicious-code-in-a-java-application
   // https://docs.oracle.com/en/java/javase/15/security/permissions-jdk1.html#GUID-8D0E8306-0DD8-4802-A71E-CFEE9BF8A287
   static class ScriptClassLoader extends URLClassLoader {
      private ScriptClassLoader() {
         super(new URL[]{});
      }

      @Override
      protected PermissionCollection getPermissions(CodeSource codesource) {
         if (codesource == null) {
            throw new NullPointerException();
         }
         PermissionCollection permissionsAllowList = new Permissions();
         permissionsAllowList.add(new RuntimePermission("accessDeclaredMembers"));
         return permissionsAllowList;
      }
   }

}
