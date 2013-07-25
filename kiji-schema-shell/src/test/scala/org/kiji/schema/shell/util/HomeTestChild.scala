/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.shell.util

final class HomeTestChild

/**
 * A program to be run in a child process.
 * This checks that $HOME is set in its environment (it received the env
 * properly from the parent process) and that it matches ${user.home}.
 * Exits with status 0 if so, status 1 otherwise.
 */
object HomeTestChild {
  def main(argv: Array[String]) {
    val homeEnv: String = System.getenv("HOME")
    val homeProp: String = System.getProperty("user.home")

    System.out.println("$HOME is " + homeEnv);
    System.out.println("${user.home} is " + homeProp)

    if (null == homeEnv || null == homeProp) {
      // One or more of these was not set and propagated correctly to the child.
      System.exit(1)
    }

    if (homeEnv != homeProp) {
      // The $HOME environment variable was set, but clobbered somehow.
      System.exit(1)
    }

    System.exit(0)
  }
}
