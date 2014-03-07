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

package org.kiji.schema.shell.spi

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.PrintStream

import org.specs2.mutable._

import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell._
import org.kiji.schema.shell.ddl._
import org.kiji.schema.shell.input.NullInputSource

/** Tests that parser plugins extended with EnvironmentPlugin work. */
class TestEnvironmentPlugin extends SpecificationWithJUnit {
  "Environment plugins" should {
    "EnvPlugin test impl should pass the PPTK" in {
      new ParserPluginTestKit(classOf[EnvPluginFactoryImpl]).testAll

      ok("Completed test")
    }

    "not respond to commands when not loaded" in {
      val parser = getParser(new ByteArrayOutputStream)
      val res = parser.parseAll(parser.statement, "ENV 'myk' 'myv';")
      res.successful mustEqual false
    }

    "respond to SHOW MODULES command" in {
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "SHOW MODULES;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[ShowModulesCommand]
      val env = res.get.exec()

      val outText = output.toString("UTF-8")

      // Check that the module name is in there.
      outText.contains("env") mustEqual true
    }

    "respond to MODULE command" in {
      val output = new ByteArrayOutputStream

      // Load the first module.
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE env;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "SHOW MODULES;")
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[ShowModulesCommand]
      res2.get.exec()

      val outText = output.toString("UTF-8")
      outText.contains("* env") mustEqual true
    }

    "respond to stateful commands in sequence" in {
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE env;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "ENV 'mykey' 'myval';")
      res2.successful mustEqual true
      val env2 = res2.get.exec()

      // Test that mykey -> myval is in the new environment mapping.
      val envPluginState2: Map[String, String] = env2.extensionMapping("env")
          .asInstanceOf[Map[String, String]] // handled for clients by getExtensionState().
      val envValue: String = envPluginState2("mykey")
      envValue mustEqual "myval"

      // Test that the next DDL command can access this state too.
      val parser3 = new DDLParser(env2)
      val res3 = parser3.parseAll(parser3.statement, "ENV 'mykey';")
      res3.successful mustEqual true
      val env3 = res3.get.exec()

      val outText3 = output.toString("UTF-8")
      outText3.contains("mykey = myval") mustEqual true

      // Test that we can override data in the environment mapping.
      val parser4 = new DDLParser(env3)
      val res4 = parser4.parseAll(parser4.statement, "ENV 'mykey' 'val2';")
      res4.successful mustEqual true
      val env4 = res4.get.exec()

      // Test that mykey -> val2 is in the new environment mapping.
      val envPluginState4: Map[String, String] = env4.extensionMapping("env")
          .asInstanceOf[Map[String, String]] // handled for clients by getExtensionState().
      val envValue4: String = envPluginState4("mykey")
      envValue4 mustEqual "val2"

      val parser5 = new DDLParser(env4)
      val res5 = parser5.parseAll(parser5.statement, "ENV 'mykey';")
      res5.successful mustEqual true
      val env6 = res5.get.exec()

      val outText5 = output.toString("UTF-8")
      outText5.contains("mykey = val2") mustEqual true
    }
  }

  def getParser(out: OutputStream): DDLParser = {
    val defaultURI = KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()
    new DDLParser(new Environment(
        defaultURI,
        new PrintStream(out),
        new MockKijiSystem(),
        new NullInputSource,
        List(),
        false))
  }
}
