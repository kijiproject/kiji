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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.shell.DDLParserHelpers
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand
import org.kiji.schema.shell.spi.ParserPlugin

/**
 * Identifies types that represent modeling lifecycle phases.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] sealed trait LifeCyclePhase

/**
 * A singleton instance representing the extract phase of a modeling lifecycle.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] object ExtractPhase extends LifeCyclePhase

/**
 * A singleton instance representing the score phase of a modeling lifecycle.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] object ScorePhase extends LifeCyclePhase

/**
 * Identifies types that specify how different configurations for a modeling lifecycle (such
 * as the model definition and environment) should be loaded.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] sealed trait ConfigureVia

/**
 * Specifies how to load different conigurations for a modeling lifecycle from a file.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] case class ConfigureViaFile(val filePath: String) extends ConfigureVia

@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] case class JobsConfiguration(
    val libjars: List[String],
    val configurationProperties: Map[String, String]
)
/**
 * Parses statements that specify how to run modeling lifecycle phases. The result of parsing a
 * complete statement is a [[org.kiji.modeling.shellext.LifecycleExecutionCommand]] capable of
 * executing a series of modeling lifecycle phases.
 *
 * @param env that schema shell statements should be executed in.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
final class ModelingParserPlugin(val env: Environment) extends ParserPlugin with DDLParserHelpers {

  /**
   * Parses a single-quoted string (i.e. of the form 'quoted string') mean to specify a file path.
   *
   * @return a parser for single-quoted strings that should be used to parse file paths.
   */
  def fs_path: Parser[String] = singleQuotedString

  /**
   * Parses a clause which specifies how to load a configuration (e.g. from a file, a Kiji table,
   * etc.). Currently, configurations can be loaded from files.
   *
   *@return a parser for clauses that specify how configurations should be loaded.
   */
  def load_configuration_clause: Parser[ConfigureVia] = {
    i("INFILE") ~> fs_path ^^ { path => ConfigureViaFile(path) }
  }

  /**
   * Parses a clause which specifies a model environment to use when running modeling
   * lifecycle phases.
   *
   * @return a parser for clauses that specify a model environment to use when executing
   *     modeling lifecycle phases.
   */
  def model_environment: Parser[ConfigureVia] = {
    i("USING") ~> i("MODEL") ~> i("ENVIRONMENT") ~> load_configuration_clause
  }

  /**
   * Parses a clause which specifies a model definition to use when running modeling lifecycle
   * phases.
   *
   * @return a parser for clauses that specify a model definition to use when executing modeling
   *     lifecycle phases.
   */
  def model_definition: Parser[ConfigureVia] = {
    i("USING") ~> i("MODEL") ~> i("DEFINITION") ~> load_configuration_clause
  }

  /**
   * Parses a clause which identifies a list of modeling lifecycle phases to execute.
   *
   * @return a parser for clauses that specify a list of modeling lifecycle phases to execute.
   */
  def phases: Parser[List[LifeCyclePhase]] = {
    i("EXTRACT") ~> i("SCORE") ^^ { _ => List(ExtractPhase, ScorePhase) }
  }

  /**
   * Defines a single (key, value) pair in a {{#properties}} list.
   */
  def property: Parser[(String, String)] = {
    singleQuotedString ~ ("=" ~> singleQuotedString) ^^ { case k ~ v => (k, v) }
  }

  /**
   * An optional `PROPERTIES ( ... )` clause in a model execution statement that lets you specify
   * properties to be included in the Hadoop configuration of jobs launched.
   *
   * @return a string-to-string map of keys and values. If this clause is completely
   *     omitted, returns an empty map.
   */
  def properties_clause: Parser[Map[String, String]] = {
    i("PROPERTIES") ~> "(" ~> repsep(property, ",") <~ ")" ^^ {
      case propList: List[(String, String)] => {
        // Convert the list of properties into a map.
        propList.toMap
      }
    } | success(Map[String, String]())
  }

  /**
   * An optional clause that can be used to specify a comma-separated list of single-quoted URIs
   * to library jars that should be placed on the distributed cache of jobs launched while
   * executing the modeling lifecycle phases.
   *
   * @return a list of string URIs to jars that should be placed on the distributed cache of jobs
   *     run while executing the modeling lifecycle phases.
   */
  def libjars_clause: Parser[List[String]] = {
    i("LIBJARS") ~> "(" ~> repsep(singleQuotedString, ",") <~ ")" | success(List())
  }

  /**
   * An optional clause that can be used to specify configuration for jobs launched while
   * executing modeling lifecycle phases.
   *
   * @return configuration for jobs launched while executing modeling lifecycle phases.
   */
  def jobs_configuration: Parser[JobsConfiguration] = {
    libjars_clause ~ properties_clause ^^ { case libjarsList ~ configMap =>
      JobsConfiguration(libjarsList, configMap)
    } | success(JobsConfiguration(List(), Map()))
  }

  /**
   * Parses a statement which specifies a list of lifecycle phases to execute using a model
   * definition and model environment.
   *
   * @return a parser for statements that specify a list of lifecycle phases to execute using a
   *     model definition and environment.
   */
  def lifecycle_execution_cmd: Parser[DDLCommand] = {
    phases ~ model_definition ~ model_environment ~ jobs_configuration ^^ {
      case phasesList ~ modelDefinition ~ modelEnvironment ~ jobsConfig => {
        new LifecycleExecutionCommand(phasesList, modelDefinition, modelEnvironment,
            jobsConfig, env)
      }
    }
  }

  override def command(): Parser[DDLCommand] = lifecycle_execution_cmd <~ ";"
}
