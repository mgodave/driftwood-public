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

import javax.script.Compilable;
import javax.script.Invocable;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.concurrent.Callable;

public class DriftwoodScripting implements Callable<Void> {
   private static String KOTLIN = "kts";

   public static void main(String[] args) throws ScriptException {
      var x = new DriftwoodScripting();
      x.call();
   }

   @Override
   public Void call() throws ScriptException {
      var manager = new ScriptEngineManager();
      var engine = manager.getEngineByExtension(KOTLIN);
      var compilable = (Compilable) engine;
      compilable.compile("fun x(a: Int): Int { return a }").eval();
      var invocable = (Invocable) engine;

      interface Test {
         int x(int a);
      }

      System.out.println("evaluating");
      System.out.println(invocable.getInterface(Test.class).x(1));
      System.out.println("evaluated");

      return null;
   }
}
