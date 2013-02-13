/**
 * (c) Copyright 2012 WibiData, Inc.
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

import java.io.PrintStream

import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.input.JLineInputSource
import org.kiji.schema.shell.input.InputSource

/**
 * Runtime environment in which DDL commands are executed.
 */
class Environment(
    val instanceURI: KijiURI =
        KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build(),
    val printer: PrintStream = Console.out,
    val kijiSystem: AbstractKijiSystem = new KijiSystem,
    val inputSource: InputSource = new JLineInputSource) {

  /**
   * @return a new Environment with the instance name replaced with 'newInstance'.
   */
  def withInstance(newInstance: String): Environment = {
    new Environment(KijiURI.newBuilder(instanceURI).withInstanceName(newInstance).build(),
        printer, kijiSystem, inputSource)
  }

  /**
   * @return a new Environment with the printer replaced with 'newPrinter'.
   */
  def withPrinter(newPrinter: PrintStream): Environment = {
    new Environment(instanceURI, newPrinter, kijiSystem, inputSource)
  }

  def withInputSource(newSource: InputSource): Environment = {
    new Environment(instanceURI, printer, kijiSystem, newSource)
  }

  /**
   * @param tableName the name of the table to test for.
   * @return true if a table named 'tableName' is present in the system.
   */
  def containsTable(tableName: String): Boolean = {
    kijiSystem.getTableLayout(instanceURI, tableName) match {
      case Some(_) => true
      case None => false
    }
  }
}
