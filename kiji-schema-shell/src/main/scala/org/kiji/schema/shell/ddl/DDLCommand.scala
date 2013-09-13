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

package org.kiji.schema.shell.ddl

import scala.collection.JavaConversions._

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.TableNotFoundException
import org.kiji.schema.shell.spi.EnvironmentPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory

/**
 * Abstract base class for DDL command implementations.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Extensible
abstract class DDLCommand {

  /**
   * Get the environment in which the command should be executed.
   *
   * <p>This must return the same Environment for the lifetime of a `DDLCommand`.
   *
   * @return the environment in which the command is executed.
   */
  def env(): Environment

  /**
   * Method called by the runtime to execute this parsed command.
   * @return the environment object to use in subsequent commands.
   */
  def exec(): Environment

  /** Return the Kiji instance name being operated on. */
  final def getKijiURI(): KijiURI = {
    env.instanceURI
  }

  /**
   * Print the supplied string to the output with a newline. Output is typically
   * stdout, but can be redirected e.g. for testing.
   * @param s the string to emit.
   */
  final protected def echo(s: String): Unit = {
    env.printer.println(s)
  }

  /**
   * Print the supplied string to the output with no trailing newline. Output is typically
   * stdout, but can be redirected e.g. for testing.
   * @param s the string to emit.
   */
  final protected def echoNoNL(s: String): Unit = {
    env.printer.print(s)
  }

  /**
   * For interactive users, print the specified prompt message and ensure that the
   * user returns an affirmative response. This throws DDLException if they don't.
   * A non-interactive environment will silently affirm.
   *
   * @param message to display to the user in an interactive terminal.
   * @throws DDLException if the user responds 'no'.
   */
  final protected def checkConfirmationPrompt(message: String): Unit = {
    if (!env.isInteractive) {
      return // non-interactive user doesn't get a prompt.
    }

    echo(message)
    val maybeInput = env.inputSource.readLine("y/N> ")
    maybeInput match {
      case None => { throw new DDLException("User canceled operation.") /* out of input. */ }
      case Some(input) => {
        if (input.toUpperCase == "Y" || input.toUpperCase == "YES") {
          return // Got confirmation from the user.
        } else {
          throw new DDLException("User canceled operation. (Please respond 'y' or 'n'.)")
        }
      }
    }
  }

  // For cases where DDLCommand instances have been created by a ParserPlugin,
  // keep a reference to the ParserPluginFactory. If it also extends EnvironmentPlugin,
  // the command can use this in getExtensionState() and updateExtensionState()
  // to access and update its additional environment data.
  private var mCurrentExtensionModule: Option[ParserPluginFactory] = None

  /**
   * Returns the environment extension associated with the current extension module.
   *
   * If this DDLCommand is the output of a ParserPlugin whose ParserPluginFactory
   * is also an EnvironmentPlugin, return the environment extension data associated
   * with the plugin. If this is not from a ParserPlugin, or the plugin does not extend
   * the environment, throws DDLException.
   *
   * @return the current plugin's environment extension data.
   * @throws DDLException if this is not running in a plugin, or the plugin does not
   *     extend EnvironmentPlugin.
   *
   */
  final protected def getExtensionState[T](): T = {
    mCurrentExtensionModule match {
      case None => throw new DDLException("This DDLCommand is not being run from an extension")
      case Some(pluginFactory) => {
        if (pluginFactory.isInstanceOf[EnvironmentPlugin[_]]) {
          try {
            // This plugin has environment data; return it, typecasting to the user's
            // specified state type.
            return env.extensionMapping(pluginFactory.getName()).asInstanceOf[T]
          } catch {
            case nsee: NoSuchElementException =>
              throw new DDLException("No extension data associated with plugin "
                  + pluginFactory.getName())
          }
        } else {
          throw new DDLException("The module " + pluginFactory.getName()
              + " does not extend EnvironmentPlugin")
        }
      }
    }
  }

  /**
   * Updates the environment extension data associated with the current plugin.
   *
   * This will return a new Environment object that contains the updated state information
   * for the current module.
   *
   * @return a new Environment containing the updated extension state for the plugin associated
   *     with this DDLCommand.
   * @throws DDLException if this is not being run from a plugin, or the plugin does not extend
   *     EnvironmentPlugin.
   */
  final protected def setExtensionState[T](newState: T): Environment = {
    mCurrentExtensionModule match {
      case None => throw new DDLException("This DDLCommand is not being run from an extension")
      case Some(pluginFactory) => {
        if (pluginFactory.isInstanceOf[EnvironmentPlugin[_]]) {
          return env.updateExtension(pluginFactory.asInstanceOf[EnvironmentPlugin[T]], newState)
        } else {
          throw new DDLException("The module " + pluginFactory.getName()
              + " does not extend EnvironmentPlugin")
        }
      }
    }
  }

  /**
   * A method used by the parser to tell a newly-created DDLCommand which plugin, if any,
   * generated the DDLCommand instance.
   *
   * The main DDL parser, or a `ParserPlugin`, can both create DDLCommand instances.
   * If this was generated by a ParserPlugin, the associated `ParserPluginFactory` is
   * recorded here, so the DDLCommand instance has access to the extension state of
   * this plugin.
   *
   * @param plugin that created this DDLCommand instance.
   */
  final private[shell] def setCurrentPlugin(plugin: ParserPluginFactory): Unit = {
    mCurrentExtensionModule = Some(plugin)
  }

}
