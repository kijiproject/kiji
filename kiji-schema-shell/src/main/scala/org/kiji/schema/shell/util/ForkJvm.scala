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

import java.io.File
import java.io.PrintStream

import scala.collection.mutable.Buffer
import scala.sys.process._ // Include the process builder/execution DSL.

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.JarLocation
import org.kiji.schema.shell.LocalJarFile

/**
 * Utility for DDLCommands that need to run code in a subprocess. This trait
 * defines one method `forkJvm()` that allows the caller to invoke
 * an arbitrary main() method in a Java subprocess.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait ForkJvm {

  /**
   * Spawns a Java subprocess to run the specified main method with an argv array.
   *
   * <p>This method spawns a Java process with the following specification:</p>
   * <ul>
   *   <li>The Java process will be invoked from the `bin/java` program in
   *       the `java.home` system property.</li>
   *   <li>The current operating system environment variables for this process will be sent
   *       to the subprocess.</li>
   *   <li>stdout and stderr will be forwarded from the current process' handles, although you
   *       may override this and capture stdout if you specify the stdout argument to this method.
   *       stdin is suppressed in the subprocess.</li>
   *   <li>The subprocess' main method will be specified by the mainMethod parameter.</li>
   *   <li>The classpath will include all libJars from the specified environment, followed
   *       by the entries in this process' java.class.path system property.</li>
   *   <li>No specific Java system properties are specified by default. System properties
   *       may be set by specifying them using `"-Dprop=val"` entries in the jvmArgs
   *       argument.</li>
   * </ul>
   *
   * <p>This method blocks until the subprocess terminates, and then returns its exit status.</p>
   *
   * @param env the current Kiji shell environment.
   * @param mainClass the name of the main Java class to run in the subprocess.
   * @param jvmArgs any arguments to specify to the JVM itself (e.g., system properties).
   *     The `-classpath` argument will be provided by this method, but you may
   *     include other arguments here if you wish.
   * @param userArgs the list of argv elements to forward to the main class.
   * @param stdout the PrintStream to use for stdout (e.g., System.out).
   * @return the exit status from the child process. Per POSIX, 0 is success, nonzero is failure.
   */
  def forkJvm(env: Environment, mainClass: String, jvmArgs: List[String],
      userArgs: List[String], stdout: PrintStream = System.out): Int = {

    val argv: Buffer[String] = Buffer() // Construct the argv to execute in this buffer.

    // Start with the path to $JAVA_HOME/bin/java.
    val javaHome: String = System.getProperty("java.home")
    val dirSep: String = System.getProperty("file.separator")
    val javaBin: String = javaHome + dirSep + "bin" + dirSep + "java"

    argv.append(javaBin)

    // If there is a boot classpath (e.g., including scala), add it to argv here.
    val bootClassPath: String = Option(System.getProperty("sun.boot.class.path")).getOrElse("")
    if (!bootClassPath.isEmpty()) {
      // Using '/a:', specify that the elements listed here are to be appended to the JVM's
      // internal bootclasspath. (See 'java -X -help')
      argv.append("-Xbootclasspath/a:" + bootClassPath)
    }

    // Child JVM's classpath contains the libJars, followed by our own classpath.
    val sysClasspath: String = System.getProperty("java.class.path")
    val libJarClasspath: String = libJarsToClasspath(env.libJars) // Terminated by ':' or empty.
    val childClasspath: String = libJarClasspath + sysClasspath

    argv.append("-classpath")
    argv.append(childClasspath)

    // Add user jvm args, then the main class to execute, followed by the user args.
    argv.appendAll(jvmArgs)
    argv.append(mainClass)
    argv.appendAll(userArgs)

    // At this point, the buffer should contain something like
    // $JAVA_HOME/bin/java -Xbootclasspath/a:...scala-rt.jar -classpath foo.jar:bar.jar \
    //     jvmarg1 jvmarg2 jvmarg3... MyMain arg1 arg2 arg3...

    // Redirect the subprocess' stdout and stderr to our own, unless the caller of this method
    // specified a non-default value for the "stdout" argument.
    val outputLogger: ProcessLogger = ProcessLogger(
        line => stdout.println(line),
        line => System.err.println(line))

    // The ProcessBuilder object in the scala.sys.process package lets you run
    // someSeq! to block and return the exit code. The current OS environment variables
    // are exported to the subprocess. and the I/O streams are redirected through the outputLogger.
    return argv ! outputLogger
  }

  /**
   * Process a set of JarLocation objects and reify this to a string suitable for use in
   * a Java `-classpath` argument.
   *
   * @param libJars is list of JarLocation objects to reify, i.e., from the current Environment.
   * @return a colon-separated list of paths to local jar files to load on the classpath.
   *     If non-empty, this list will terminate with a ':' character.
   */
  private[shell] def libJarsToClasspath(libJars: List[JarLocation]): String = {
    val localPaths: List[String] = libJars.map({ libJar: JarLocation =>
      libJar match {
        case LocalJarFile(path) => new File(path).getAbsolutePath()
      }
    })

    // Delimiter between classpath entries, ":" on most systems.
    val pathSep: String = System.getProperty("path.separator")

    // Fold over the individual elements, concatenating them, delimited by ':' characters.
    // Use foldRight so it terminates in a ":" rather than starts with one.
    val finalClasspath: String = localPaths.foldRight("")({ (classpath: String, path: String) =>
      classpath + pathSep + path
    })

    return finalClasspath
  }
}
