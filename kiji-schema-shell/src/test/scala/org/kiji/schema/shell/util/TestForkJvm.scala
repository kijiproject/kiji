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

import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell._
import org.kiji.schema.shell.ddl._
import org.kiji.schema.shell.input.NullInputSource
import org.kiji.schema.shell.spi._

/** Tests that DDLCommands extended with ForkJvm work. */
class TestForkJvm extends SpecificationWithJUnit {
  "Environment plugins" should {
    "ForkTestPlugin test impl should pass the PPTK" in {
      new ParserPluginTestKit(classOf[ForkTestPluginFactoryImpl]).testAll

      ok("Completed test")
    }

    "fork a child JVM and run the correct main() method" in {
      val parser = getParser(System.out)
      val res = parser.parseAll(parser.statement, "MODULE addfour;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      // This runs the command. It will fork a subprocess to run the PlusFourChild
      // program. This program will check its exit status and will throw a DDLException
      // if it does not match the expected value.
      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, "ADDFOUR;")
      res2.successful mustEqual true
      // Successful completion of this statement means we forked and joined
      // a subprocess correctly.
      val env2 = res2.get.exec()

      env2 mustEqual env // This command should not change the environment.
    }

    "fork a child JVM using the FORK JVM command and test that the env is xmitted ok" in {
      val parser = getParser(System.out)
      val res = parser.parseAll(parser.statement, "MODULE fork;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      // This runs the command. It will fork a subprocess to run the PlusFourChild
      // program. This program will check its exit status and will throw a DDLException
      // if it does not match the expected value.
      val parser2 = new DDLParser(env)
      val mainClass: String = new HomeTestChild().getClass().getName()
      val res2 = parser2.parseAll(parser2.statement, "FORK JVM '" + mainClass + "';")
      res2.successful mustEqual true
      // Successful completion of this statement means we forked and joined
      // a subprocess correctly and it exited with status 0.
      val env2 = res2.get.exec()

      env2 mustEqual env // This command should not change the environment.
    }

    "fork a child JVM and capture its stdout" in {
      val parser = getParser(System.out)
      val res = parser.parseAll(parser.statement, "MODULE fork;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      val env = res.get.exec()

      class ForkClass extends ForkJvm

      val forker: ForkClass = new ForkClass
      // This prints "hello\nworld\n" on stdout.
      val mainClass: String = new HelloWorldChild().getClass().getName
      val bytesOut: ByteArrayOutputStream = new ByteArrayOutputStream()
      val outWrapper: PrintStream = new PrintStream(bytesOut)
      val childRet: Int = forker.forkJvm(env, mainClass, List(), List(), outWrapper)

      childRet mustEqual 0

      outWrapper.close()
      bytesOut.close()
      val finalText = bytesOut.toString()
      val expectedText = "hello\nworld\n"
      finalText mustEqual expectedText
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
