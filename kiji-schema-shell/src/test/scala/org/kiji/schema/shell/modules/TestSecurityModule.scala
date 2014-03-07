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
package org.kiji.schema.shell.modules

import org.scalatest.mock.EasyMockSugar
import org.specs2.mutable.SpecificationWithJUnit

import org.kiji.schema.KijiURI
import org.kiji.schema.security.KijiPermissions
import org.kiji.schema.security.KijiUser
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.MockKijiSystem
import org.kiji.schema.shell.input.NullInputSource
import org.kiji.schema.shell.spi.ParserPluginTestKit
import org.kiji.schema.shell.util.KijiTestHelpers
import org.kiji.schema.shell.ddl.UseModuleCommand

/**
 * Tests the security module's parsing through actions, with a mocked KijiSecurityManager from
 * MockKijiSystem.
 */
class TestSecurityModule
    extends SpecificationWithJUnit
    with KijiTestHelpers
    with EasyMockSugar {

  "SecurityModule" should {
    "pass the PPTK" in {
      new ParserPluginTestKit(classOf[SecurityParserPluginFactory]).testAll

      ok("Completed test")
    }

    "Take correct actions on a GRANT command" in {
      val testInstanceURI = getNewInstanceURI()
      val environment = env(testInstanceURI)
      val username = "daisy"
      val grantCommand = "GRANT READ PRIVILEGES ON INSTANCE '%s' TO USER %s;".format(
        testInstanceURI.toString,
        username)
      val enableSecurityModuleCommand =
        "MODULE security;"

      val mockSecurityManager = environment.kijiSystem.getSecurityManager(testInstanceURI)

      expecting {
        mockSecurityManager.grant(KijiUser.fromName(username), KijiPermissions.Action.READ)
      }

      whenExecuting(mockSecurityManager) {
        val parser1 = getParser(environment)
        val res1 = parser1.parseAll(parser1.statement, enableSecurityModuleCommand)
        res1.successful mustEqual true
        res1.get must beAnInstanceOf[UseModuleCommand]
        val env1 = res1.get.exec()
        val parser2 = getParser(env1)
        val res2 = parser2.parseAll(parser2.statement, grantCommand)
        res2.successful mustEqual true
        res2.get.exec()
      }

      ok("Completed test")
    }

    "Take correct actions on a REVOKE command" in {
      val testInstanceURI = getNewInstanceURI()
      val environment = env(testInstanceURI)
      val username = "daisy"
      val revokeCommand = "REVOKE READ PRIVILEGES ON INSTANCE '%s' FROM USER %s;".format(
        testInstanceURI.toString,
        username)
      val enableSecurityModuleCommand =
        "MODULE security;"

      val mockSecurityManager = environment.kijiSystem.getSecurityManager(testInstanceURI)

      expecting {
        mockSecurityManager.revoke(KijiUser.fromName(username), KijiPermissions.Action.READ)
      }

      whenExecuting(mockSecurityManager) {
        val parser1 = getParser(environment)
        val res1 = parser1.parseAll(parser1.statement, enableSecurityModuleCommand)
        res1.successful mustEqual true
        res1.get must beAnInstanceOf[UseModuleCommand]
        val env1 = res1.get.exec()
        val parser2 = getParser(env1)
        val res2 = parser2.parseAll(parser2.statement, revokeCommand)
        res2.successful mustEqual true
        res2.get.exec()
      }

      ok("Completed test")
    }

    "Take correct actions on a GRANT command with multiple actions" in {
      val testInstanceURI = getNewInstanceURI()
      val environment = env(testInstanceURI)
      val username = "daisy"
      val grantCommand = "GRANT READ WRITE PRIVILEGES ON INSTANCE '%s' TO USER %s;".format(
        testInstanceURI.toString,
        username)
      val enableSecurityModuleCommand =
        "MODULE security;"

      val mockSecurityManager = environment.kijiSystem.getSecurityManager(testInstanceURI)

      expecting {
        mockSecurityManager.grant(KijiUser.fromName(username), KijiPermissions.Action.READ)
        mockSecurityManager.grant(KijiUser.fromName(username), KijiPermissions.Action.WRITE)
      }

      whenExecuting(mockSecurityManager) {
        val parser1 = getParser(environment)
        val res1 = parser1.parseAll(parser1.statement, enableSecurityModuleCommand)
        res1.successful mustEqual true
        res1.get must beAnInstanceOf[UseModuleCommand]
        val env1 = res1.get.exec()
        val parser2 = getParser(env1)
        val res2 = parser2.parseAll(parser2.statement, grantCommand)
        res2.successful mustEqual true
        res2.get.exec()
      }

      ok("Completed test")
    }

    "Take correct actions on a REVOKE command with multiple actions" in {
      val testInstanceURI = getNewInstanceURI()
      val environment = env(testInstanceURI)
      val username = "daisy"
      val revokeCommand = "REVOKE READ WRITE PRIVILEGES ON INSTANCE '%s' FROM USER %s;".format(
          testInstanceURI.toString,
          username)
      val enableSecurityModuleCommand =
        "MODULE security;"

      val mockSecurityManager = environment.kijiSystem.getSecurityManager(testInstanceURI)

      expecting {
        mockSecurityManager.revoke(KijiUser.fromName(username), KijiPermissions.Action.READ)
        mockSecurityManager.revoke(KijiUser.fromName(username), KijiPermissions.Action.WRITE)
      }

      whenExecuting(mockSecurityManager) {
        val parser1 = getParser(environment)
        val res1 = parser1.parseAll(parser1.statement, enableSecurityModuleCommand)
        res1.successful mustEqual true
        res1.get must beAnInstanceOf[UseModuleCommand]
        val env1 = res1.get.exec()
        val parser2 = getParser(env1)
        val res2 = parser2.parseAll(parser2.statement, revokeCommand)
        res2.successful mustEqual true
        res2.get.exec()
      }

      ok("Completed test")
    }

    "Take correct actions on a GRANT command without optional keywords in the command" in {
      val testInstanceURI = getNewInstanceURI()
      val environment = env(testInstanceURI)
      val username = "daisy"
      val grantCommand = "GRANT READ WRITE ON INSTANCE '%s' TO %s;".format(
        testInstanceURI.toString,
        username)
      val enableSecurityModuleCommand =
        "MODULE security;"

      val mockSecurityManager = environment.kijiSystem.getSecurityManager(testInstanceURI)

      expecting {
        mockSecurityManager.grant(KijiUser.fromName(username), KijiPermissions.Action.READ)
        mockSecurityManager.grant(KijiUser.fromName(username), KijiPermissions.Action.WRITE)
      }

      whenExecuting(mockSecurityManager) {
        val parser1 = getParser(environment)
        val res1 = parser1.parseAll(parser1.statement, enableSecurityModuleCommand)
        res1.successful mustEqual true
        res1.get must beAnInstanceOf[UseModuleCommand]
        val env1 = res1.get.exec()
        val parser2 = getParser(env1)
        val res2 = parser2.parseAll(parser2.statement, grantCommand)
        res2.successful mustEqual true
        res2.get.exec()
      }

      ok("Completed test")
    }
  }

  /**
   * Get an Environment instance.
   */
  def env(instanceURI: KijiURI): Environment = {
    new Environment(
      instanceURI = instanceURI,
      printer = System.out,
      kijiSystem = new MockKijiSystem,
      inputSource = new NullInputSource(),
      modules = List(),
      isInteractive = false,
      libJars = List())
  }
}
