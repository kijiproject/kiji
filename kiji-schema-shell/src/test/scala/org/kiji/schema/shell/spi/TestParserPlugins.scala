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

/** Tests that parser plugins work. */
class TestParserPlugins extends SpecificationWithJUnit {
  "Parser plugin modules" should {
    "Test impl1 should pass the PPTK" in {
      new ParserPluginTestKit(classOf[PluginFactoryImpl1]).testAll

      ok("Completed test")
    }

    "Test impl2 should pass the PPTK" in {
      new ParserPluginTestKit(classOf[PluginFactoryImpl2]).testAll

      ok("Completed test")
    }

    "not respond to commands when not loaded" in {
      val parser = getParser(new ByteArrayOutputStream)
      val res = parser.parseAll(parser.statement, "SET X;")
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

      // Check that both module names are in there.
      outText.contains("first") mustEqual true
      outText.contains("second") mustEqual true
    }

    "fail if you load a missing module" in {
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE thisdoesnotexist;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      res.get.exec() must throwA[DDLException]
    }

    "fail to use module-based commands without explicit load" in {
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "SET X;")
      res.successful mustEqual false
    }

    "still fail on syntax error even with a module loaded" in {
      val output = new ByteArrayOutputStream

      // Load the first module.
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE first;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "NOT A VALID SYNTAX HERE;")
      res2.successful mustEqual false
    }

    "respond to MODULE command" in {
      val output = new ByteArrayOutputStream

      // Load the first module.
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE first;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "SHOW MODULES;")
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[ShowModulesCommand]
      res2.get.exec()

      val outText = output.toString("UTF-8")
      outText.contains("*") mustEqual true
    }

    "respond to SET X command" in {
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE first;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "SET X;")
      res2.successful mustEqual true
      res2.get.exec()

      val outText = output.toString("UTF-8")
      outText.contains("X is now 1") mustEqual true
    }

    "respond to alternate SET X command" in {
      // Module 'second' manipulates a string, not an integer with SET X.
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE second;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "SET X;")
      res2.successful mustEqual true
      res2.get.exec()

      val outText = output.toString("UTF-8")
      outText.contains("X is now x") mustEqual true
    }

    "demonstrate that order matters when loading modules" in {
      // Module 'second' manipulates a string, not an integer with SET X.
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE second;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "MODULE first;")
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[UseModuleCommand]
      val env2 = res2.get.exec()

      val parser3 = new DDLParser(env2)
      val res3 = parser3.parseAll(parser3.statement, "SET X;")
      res3.successful mustEqual true
      res3.get.exec()

      val outText = output.toString("UTF-8")
      outText.contains("X is now x") mustEqual true
    }

    "demonstrate that backtracking works across module syntaxes" in {
      // Module 'second' manipulates things with SET X.
      val output = new ByteArrayOutputStream
      val parser = getParser(output)
      val res = parser.parseAll(parser.statement, "MODULE second;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "MODULE first;")
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[UseModuleCommand]
      val env2 = res2.get.exec()

      val parser3 = new DDLParser(env2)
      val res3 = parser3.parseAll(parser3.statement, "SET X;")
      res3.successful mustEqual true
      val env3 = res3.get.exec()

      val outText = output.toString("UTF-8")
      outText.contains("X is now x") mustEqual true

      val parser4 = new DDLParser(env3)
      val res4 = parser4.parseAll(parser4.statement, "SET Y;")
      res4.successful mustEqual true
      res4.get.exec()

      val outText2 = output.toString("UTF-8")
      outText2.contains("Y is now 1") mustEqual true
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
