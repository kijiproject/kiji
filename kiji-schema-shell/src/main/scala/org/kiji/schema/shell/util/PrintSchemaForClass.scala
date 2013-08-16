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

import java.lang.reflect.Field

import org.kiji.annotations.ApiAudience

/**
 * A program that can be run in a subprocess to print (on stdout) the schema associated
 * with a SpecificRecord class instance.
 *
 * <p>Takes as argv a single class name which must be a SpecificRecord.</p>
 */
@ApiAudience.Private
final class PrintSchemaForClass

object PrintSchemaForClass {
  def main(argv: Array[String]) {
    // Avro SpecificRecord classes have a static field named SCHEMA$. Print its value to stdout.
    val className: String = argv(0)
    val classObj: Class[_] = Class.forName(className)
    val schemaField: Field = classObj.getField("SCHEMA$")
    schemaField.setAccessible(true)
    System.out.println(schemaField.get(null).toString())
  }
}



