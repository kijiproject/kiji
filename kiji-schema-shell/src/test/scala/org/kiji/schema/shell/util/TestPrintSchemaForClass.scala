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

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.PrintStream

import org.specs2.mutable._

import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.shell._
import org.kiji.schema.shell.ddl._
import org.kiji.schema.shell.input.NullInputSource
import org.kiji.schema.shell.spi._

/** Tests the PrintSchemaForClass utility. */
class TestPrintSchemaForClass extends SpecificationWithJUnit {
  "PrintSchemaForClass" should {
    "properly emit a schema for an Avro SpecificRecord class" in {
      val forker: ForkClass = new ForkClass
      // This prints the SCHEMA$ of TableLayoutDesc on stdout
      val mainClass: String = new PrintSchemaForClass().getClass().getName
      val bytesOut: ByteArrayOutputStream = new ByteArrayOutputStream()
      val outWrapper: PrintStream = new PrintStream(bytesOut)
      val argv: List[String] = List(new TableLayoutDesc().getClass().getName)
      val childRet: Int = forker.forkJvm(env, mainClass, List(), argv, outWrapper)

      // This subprocess must return exit status 0
      childRet mustEqual 0

      outWrapper.close()
      bytesOut.close()
      val finalText = bytesOut.toString()
      val expectedText = TableLayoutDesc.SCHEMA$.toString()

      System.out.println("Got class schema output:")
      System.out.println("==========")
      System.out.println(finalText)
      System.out.println("==========")

      finalText.trim() mustEqual expectedText.trim()
    }

    "exit with nonzero status if the target class does not exist" in {
      val forker: ForkClass = new ForkClass
      val mainClass: String = new PrintSchemaForClass().getClass().getName
      val argv: List[String] = List("com.nowhere.AMissingClassName")
      val childRet: Int = forker.forkJvm(env, mainClass, List(), argv)

      // This subprocess must not return exit status 0
      childRet mustNotEqual 0
    }
  }

  class ForkClass extends ForkJvm

  val env: Environment = new Environment(
        KijiURI.newBuilder("kiji://.env/default").build(),
        new PrintStream(System.out),
        new MockKijiSystem(),
        new NullInputSource,
        List(),
        false)
}
