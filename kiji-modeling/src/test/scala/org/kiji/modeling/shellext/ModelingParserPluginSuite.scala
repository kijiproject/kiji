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

package org.kiji.modeling.shellext

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.schema.shell.ddl.UseModuleCommand
import org.kiji.schema.shell.spi.ParserPluginFactory
import java.util.ServiceLoader
import java.io.{InputStreamReader, BufferedReader}

/**
 * Tests parsing clauses for the KijiExpress extension to KijiSchema DDL Shell. Such parsers are
 * defined in [[org.kiji.modeling.shellext.ModelingParserPlugin]].
 */
@RunWith(classOf[JUnitRunner])
class ModelingParserPluginSuite extends ShellExtSuite {

  private def verifyConfigureViaFile(configureViaParsed: ConfigureVia, expectedPath: String) {
    assert(configureViaParsed.isInstanceOf[ConfigureViaFile],
      "Expected configure via file, but got some other type.")
    configureViaParsed match {
      case ConfigureViaFile(filePath) => assert(expectedPath === filePath)
    }
  }

  private def verifyLifecyclePhases(phases: List[LifeCyclePhase]) {
    assert(2 === phases.size, "Parsed wrong number of lifecycle phases.")
    assert(phases(0) === ExtractPhase, "Expected extract as first lifecycle phase.")
    assert(phases(1) === ScorePhase, "Expected score as second lifecycle phase.")
  }

  test("The modeling schema-shell module can be loaded.") {
    val parser = getBaseParser()
    val result = parser.parseAll(parser.statement, "MODULE modeling;")
    assert(result.successful, "'MODULE modeling;' statement not executed successfully.")
    assert(result.get.isInstanceOf[UseModuleCommand],
        "Parsing 'MODULE modeling;' produced incorrect result type.")
    assert(Option(result.get.exec()).isDefined, "Executing 'MODULE modeling;' statement failed.")
  }

  test("A base configuration clause can be used to parse a file path.") {
    val parser = getParserPlugin()
    val result = parser.parseAll(parser.load_configuration_clause,
        """INFILE '/expected/file/path.json'""")
    assert(result.successful, "Parsing [INFILE '/expected/file/path.json'] failed.")
    verifyConfigureViaFile(result.get, "/expected/file/path.json" )
  }

  test("A model definition clause can be used to obtain the path to a model definition.") {
    val parser = getParserPlugin()
    val result = parser.parseAll(parser.model_definition,
        """USING MODEL DEFINITION INFILE '/expected/path/def.json'""")
    assert(result.successful,
        "Parsing [USING MODEL DEFINITION INFILE '/expected/path/def.json'] failed.")
    verifyConfigureViaFile(result.get, "/expected/path/def.json")
  }

  test("A model environment clause can be used to obtain the path to a model environment.") {
    val parser = getParserPlugin()
    val result = parser.parseAll(parser.model_environment,
        """USING MODEL ENVIRONMENT INFILE '/expected/path/env.json'""")
    assert(result.successful,
        "Parsing [USING MODEL ENVIRONMENT INFILE '/expected/path/env.json'] failed.")
    verifyConfigureViaFile(result.get, "/expected/path/env.json")
  }

  test("A phases selection clause can be used to select extract+score") {
    val parser = getParserPlugin()
    val result = parser.parseAll(parser.phases,
        """EXTRACT SCORE""")
    assert(result.successful, "Parsing [EXTRACT SCORE] failed.")
    verifyLifecyclePhases(result.get)
  }

  test("A single property can be parsed.") {
    val parser = getParserPlugin()
    val result = parser.parseAll(parser.property, """ 'key'='value' """)
    assert(result.successful, "Parsing ['key'='value'] failed.")
    assert( ("key", "value") === result.get, "Parsed incorrect property key/value.")
  }

  def verifyPropertiesMap(properties: Map[String, String]) {
    (0 to 2).foreach { i =>
      assert("value" + i === properties("key" + i), "Wrong value for key in parsed properties")
    }
  }

  test("A collection of configuration properties can be parsed.") {
    val parser = getParserPlugin()
    val result = parser.parseAll(parser.properties_clause,
        """
         |PROPERTIES ( 'key0'='value0', 'key1'='value1', 'key2'='value2' )
       """.stripMargin
    )
    assert(result.successful, "Parsing a properties list failed.")
    verifyPropertiesMap(result.get)
  }

  def verifyLibJarsList(libjars: List[String]) {
    (0 to 2).foreach { i =>
      assert("file:///path/%d.jar".format(i) === libjars(i), "Wrong libjar path parsed.")
    }
  }

  test("A list of libjars can be parsed.") {
    val parser = getParserPlugin()
    val result = parser.parseAll(parser.libjars_clause,
      """
        |LIBJARS ( 'file:///path/0.jar', 'file:///path/1.jar', 'file:///path/2.jar' )
      """.stripMargin
    )
    assert(result.successful, "Parsing a list of library jars failed.")
    verifyLibJarsList(result.get)
  }

  test("A lifecycle execution statement can be used to obtain a LifecycleExecutionCommand.") {
    val parser = getParserPlugin()
    val statement =
      """
        |EXTRACT SCORE
        |  USING MODEL DEFINITION INFILE '/expected/path/def.json'
        |  USING MODEL ENVIRONMENT INFILE '/expected/path/env.json'
      """.stripMargin
    val result = parser.parseAll(parser.lifecycle_execution_cmd, statement)
    assert(result.successful, "Parsing [%s] failed.".format(statement))
    assert(result.get.isInstanceOf[LifecycleExecutionCommand], "Parsed command of incorrect type.")
    val command: LifecycleExecutionCommand = result.get.asInstanceOf[LifecycleExecutionCommand]
    verifyConfigureViaFile(command.modelDefConfigureVia, "/expected/path/def.json")
    verifyConfigureViaFile(command.modelEnvConfigureVia, "/expected/path/env.json")
    verifyLifecyclePhases(command.lifecyclePhases)
    assert(List() === command.jobsConfiguration.libjars,
        "Parsed jobs configuration when none specified.")
    assert(Map() === command.jobsConfiguration.configurationProperties,
      "Parsed jobs configuration when none specified.")
  }

  test("A lifecycle execution statement can include a list of libjars.") {
    val parser = getParserPlugin()
    val statement =
      """
        |EXTRACT SCORE
        |  USING MODEL DEFINITION INFILE '/expected/path/def.json'
        |  USING MODEL ENVIRONMENT INFILE '/expected/path/env.json'
        |  LIBJARS ( 'file:///path/0.jar', 'file:///path/1.jar', 'file:///path/2.jar')
      """.stripMargin
    val result = parser.parseAll(parser.lifecycle_execution_cmd, statement)
    assert(result.successful, "Parsing [%s] failed.".format(statement))
    assert(result.get.isInstanceOf[LifecycleExecutionCommand], "Parsed command of incorrect type.")
    val command: LifecycleExecutionCommand = result.get.asInstanceOf[LifecycleExecutionCommand]
    verifyLibJarsList(command.jobsConfiguration.libjars)
  }

  test("A lifecycle execution statement can include a list of job config properties.") {
    val parser = getParserPlugin()
    val statement =
      """
        |EXTRACT SCORE
        |  USING MODEL DEFINITION INFILE '/expected/path/def.json'
        |  USING MODEL ENVIRONMENT INFILE '/expected/path/env.json'
        |  PROPERTIES ( 'key0'='value0', 'key1'='value1', 'key2'='value2' )
      """.stripMargin
    val result = parser.parseAll(parser.lifecycle_execution_cmd, statement)
    assert(result.successful, "Parsing [%s] failed.".format(statement))
    assert(result.get.isInstanceOf[LifecycleExecutionCommand], "Parsed command of incorrect type.")
    val command: LifecycleExecutionCommand = result.get.asInstanceOf[LifecycleExecutionCommand]
    verifyPropertiesMap(command.jobsConfiguration.configurationProperties)
  }
}
