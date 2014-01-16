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

package org.kiji.schema.shell

import scala.collection.JavaConverters._

import org.kiji.annotations.ApiAudience
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.input.FileInputSource
import org.kiji.schema.shell.input.JLineInputSource
import org.kiji.schema.shell.input.StringInputSource
import org.kiji.schema.shell.ddl.UseModuleCommand

/**
 * An object used to run a Kiji schema shell.
 */
@ApiAudience.Private
final class ShellMain {
  @Flag(name="kiji", usage="Kiji instance URI")
  var kijiURI: String = "kiji://.env/%s".format(KConstants.DEFAULT_INSTANCE_NAME)

  @Flag(name="expr", usage="Expression to execute")
  var expr: String = ""

  @Flag(name="file", usage="Script file to execute")
  var filename: String = ""

  @Flag(name="modules", usage="Comma-separated list of names of modules to pre-load.")
  var moduleNamesToPreLoad: java.util.List[String] = new java.util.ArrayList()

  /**
   * This is a def instead of a val so that it isn't evaluated before the flags vars above are set.
   *
   * @return Whether the shell loop is being run in interactive mode or not.
   */
  def interactiveMode: Boolean = expr.equals("") && filename.equals("")

  /**
   * Programmatic entry point.
   * Like main(), but without that pesky sys.exit() call.
   * @return a return status code. 0 is success.
   */
  def run(): Int = {
    val chatty = interactiveMode
    if (chatty) {
      println("Kiji schema shell v" + ShellMain.version())
      println("""Enter 'help' for instructions (without quotes).
                |Enter 'quit' to quit.
                |DDL statements must be terminated with a ';'""".stripMargin)
    }
    processUserInput()
    if (chatty) {
      println("Thank you for flying Kiji!")
    }
    0 // Return 0 if we didn't terminate with an exception.
  }

  /**
   * Obtains a new environment loaded with any modules specified on the command line.
   *
   * @param envToLoad is an environment that should be loaded with modules.
   * @return a new environment loaded with any modules specified on the command line.
   */
  private def preloadModules(envToLoad: Environment): Environment = {
    // The tool accepts multiple --modules flags, each of which should contain a comma-separated
    // list of module names to preload. We transform the comma-separated lists into a single list
    // of names, and then fold the names into an environment with each named module loaded.
    moduleNamesToPreLoad.asScala
        .flatMap { _.split(',') }
        .foldLeft(envToLoad) { (env, moduleName) =>
          (new UseModuleCommand(env, moduleName)).exec()
        }
  }

  /**
   * Create the initial Environment object that should be used to start query processing.
   */
  def initialEnv(): Environment = {
    val (input, isInteractive) = (
      if (!filename.equals("")) {
        (new FileInputSource(filename), false)
      } else if (!expr.equals("")) {
        (new StringInputSource(expr), false)
      } else {
        // Read from the interactive terminal
        (new JLineInputSource, true)
      }
    )

    val uri = KijiURI.newBuilder(kijiURI).build()
    return preloadModules(new Environment(uri, Console.out, ShellMain.shellKijiSystem, input,
        List(), isInteractive, Map(), List()))
  }

  /**
   * Request a line of user input, parse it, and execute the command.
   * Recursively continue to request the next line of user input until
   * we exhaust the input.
   *
   * @return the final environment
   */
  def processUserInput(): Environment = {
    new InputProcessor(throwOnErr = !interactiveMode)
        .processUserInput(new StringBuilder, initialEnv())
  }
}

object ShellMain {

  /** Singleton KijiSystem instance used in interactive shell process. */
  val shellKijiSystem: AbstractKijiSystem = new KijiSystem

  /**
   * @return the version number associated with this software package.
   */
  def version(): String = {
    // Uses the value of 'Implementation-Version' in META-INF/MANIFEST.MF:
    val version = Option(classOf[ShellMain].getPackage().getImplementationVersion())
    return version.getOrElse("(unknown)")
  }

  /**
   * Main entry point for running the Kiji schema shell.
   *
   * @param argv Command line arguments.
   */
  def main(argv: Array[String]) {
    val shellMain = new ShellMain()
    val nonFlagArgs: Option[java.util.List[String]] = Option(FlagParser.init(shellMain, argv));
    if (nonFlagArgs.equals(None)) {
      sys.exit(1); // There was a problem parsing flags.
    }

    val retVal = shellMain.run()

    // Close all connections properly before exiting.
    ShellMain.shellKijiSystem.shutdown()
    if (retVal != 0) {
      sys.exit(retVal)
    }
  }
}
