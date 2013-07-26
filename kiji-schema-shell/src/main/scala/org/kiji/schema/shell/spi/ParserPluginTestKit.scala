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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.delegation.NamedProvider
import org.kiji.schema.shell.Environment

/**
 * An API that allows you to unit test your {@link ParserPlugin} and {@link ParserPluginFactory}
 * implementations.
 *
 * <p>Create an instance of this class parameterized by the {@link Class} object
 * that represents your `ParserPluginFactory`. Then call the test methods
 * to test different aspects of its functionality. Call {@link #testAll} to run
 * all test methods.</p>
 *
 * <p>Each test throws {@link IllegalStateException} on error.</p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class ParserPluginTestKit(val klazz: Class[_ <: ParserPluginFactory]) {
  /** Tests that klazz is non-null. */
  def testNonNull(): Unit = {
    require(klazz != null, "Specified class under test is null.")
  }

  /** Tests we can instantiate your factory with a parameterless constructor. */
  def testInstantiation(): Unit = {
    try {
      val instance: ParserPluginFactory = klazz.newInstance
    } catch { case e: Exception =>
      throw new IllegalStateException("Cannot create a new instance of " + klazz.getName()
          + "through reflection", e)
    }
  }

  /** Tests that the getName() method returns the same value every time. */
  def testNameConstant(): Unit = {
    val instance1: ParserPluginFactory = klazz.newInstance
    val instance2: ParserPluginFactory = klazz.newInstance

    val name1 = instance1.getName
    val name2 = instance2.getName

    require(null != name1, klazz.getName() + " getName() method returns null")
    require(name1 == name2, klazz.getName() + " getName() method returns non-constant value")
  }

  /** Tests that ParserPluginFactory creates a valid ParserPlugin instance. */
  def testCreatesParserPlugin(): Unit = {
    val factory: ParserPluginFactory = klazz.newInstance
    val parser = factory.create(new Environment())
    require(null != parser, "Factory class " + klazz.getName() + " creates null parser.")
  }

  /** Tests that ParserPluginFactory.create() respects the env parameter. */
  def testCreateRespectsEnv(): Unit = {
    val factory: ParserPluginFactory = klazz.newInstance
    val env1 = new Environment()
    val env2 = new Environment()
    val parser1 = factory.create(env1)
    val parser2 = factory.create(env2)

    if (env1 == env2) {
      // Sanity check that we haven't overridden == on environments.
      sys.error("Internal error: env1 and env2 were the same")
    }

    require(env1 == parser1.env, "Factory class " + klazz.getName()
          + " create() doesn't respect the 'env' argument.")

    require(env2 == parser2.env, "Factory class " + klazz.getName()
          + " create() doesn't respect the 'env' argument on subsequent calls.")
  }

  /** Tests that the parser returned by ParserPlugin.command is non-null. */
  def testParserPluginCreatesNonNullParser(): Unit = {
    val factory: ParserPluginFactory = klazz.newInstance
    val parser = factory.create(new Environment())
    require(null != parser.command,
        "Factory class creates ParserPlugin but plugin.command returns null.")
  }

  /** Run all unit tests. */
  def testAll(): Unit = {
    testNonNull
    testInstantiation
    testNameConstant
    testCreatesParserPlugin
    testCreateRespectsEnv
    testParserPluginCreatesNonNullParser
  }
}
