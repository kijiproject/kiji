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

package org.kiji.express.modeling.config

import scala.collection.mutable.Map
import scala.io.Source

import cascading.tuple.Fields
import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.avro.AvroModelDefinition
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Tuples
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.KijiNameValidator
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson

/**
 * A ModelDefinition is a descriptor of the computational logic to use at different phases of
 * of a modeling workflow.
 *
 * A ModelDefinition can be created programmatically:
 * {{{
 * val modelDefinition = ModelDefinition(name = "name",
 *     version = "1.0.0",
 *     extractorClass = classOf[org.kiji.express.modeling.ModelDefinitionSuite.MyExtractor],
 *     scorerClass = classOf[org.kiji.express.modeling.ModelDefinitionSuite.MyScorer])
 * }}}
 *
 * Alternatively a ModelDefinition can be created from JSON. JSON model specifications should be
 * written using the following format:
 * {{{
 * {
 *   "name" : "identifier-for-this-model",
 *   "version" : "1.0.0",
 *   "extractor_class" : "com.organization.YourExtractor",
 *   "scorer_class" : "com.organization.YourScorer",
 *   "protocol_version" : "model_definition-0.1.0"
 * }
 * }}}
 *
 * To load a JSON model definition:
 * {{{
 * // Load a JSON string directly.
 * val myModelDefinition: ModelDefinition =
 *     ModelDefinition.loadJson("""{ "name": "myIdentifier", ... }""")
 *
 * // Load a JSON file.
 * val myModelDefinition2: ModelDefinition =
 *     ModelDefinition.loadJsonFile("/path/to/json/config.json")
 * }}}
 *
 * @param name of the model definition.
 * @param version of the model definition.
 * @param extractorClass to be used in the extract phase of the model definition.
 * @param scorerClass to be used in the score phase of the model definition.
 * @param protocolVersion this model definition was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ModelDefinition private[express] (
    val name: String,
    val version: String,
    val extractorClass: java.lang.Class[_ <: Extractor],
    val scorerClass: java.lang.Class[_ <: Scorer],
    private[express] val protocolVersion: ProtocolVersion =
        ModelDefinition.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model definition are valid.
  ModelDefinition.validateModelDefinition(this)

  /**
   * Serializes this model definition into a JSON string.
   *
   * @return a JSON string that represents the model definition.
   */
  final def toJson(): String = {
    // Build an AvroModelDefinition record.
    val definition: AvroModelDefinition = AvroModelDefinition
        .newBuilder()
        .setName(name)
        .setVersion(version)
        .setProtocolVersion(protocolVersion.toString)
        .setExtractorClass(extractorClass.getName)
        .setScorerClass(scorerClass.getName)
        .build()

    // Encode it into JSON.
    ToJson.toAvroJsonString(definition)
  }

  /**
   * Creates a new model definition with settings specified to this method. Any setting specified
   * to this method is used in the new model definition. Any unspecified setting will use the
   * value from this model definition in the new model definition.
   *
   * @param name of the model definition.
   * @param version of the model definition.
   * @param extractor used by the model definition.
   * @param scorer used by the model definition.
   * @return a new model definition using the settings specified to this method.
   */
  final def withNewSettings( name: String = this.name,
      version: String = this.version,
      extractor: Class[_ <: Extractor] = this.extractorClass,
      scorer: Class[_ <: Scorer] = this.scorerClass): ModelDefinition = {
    new ModelDefinition(name, version, extractor, scorer, this.protocolVersion)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case definition: ModelDefinition => {
        name == definition.name &&
            version == definition.version &&
            extractorClass == definition.extractorClass &&
            scorerClass == definition.scorerClass &&
            protocolVersion == definition.protocolVersion
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(
          name,
          version,
          extractorClass,
          scorerClass,
          protocolVersion)
}

/**
 * Companion object for ModelDefinition. Contains constants related to model definitions as well as
 * validation methods.
 */
object ModelDefinition {
  /** Maximum model definition version we can recognize. */
  val MAX_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.1.0")

  /** Minimum model definition version we can recognize. */
  val MIN_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.1.0")

  /** Current model definition protocol version. */
  val CURRENT_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.1.0")

  /** Regular expression used to validate a model definition version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model definition. */
  private[express] val VALIDATION_MESSAGE = "One or more errors occurred while validating your " +
      "model definition. Please correct the problems in your model definition and try again."

  /**
   * Creates a new model definition using the specified settings.
   *
   * @param name of the model definition.
   * @param version of the model definition.
   * @param extractor used by the model definition.
   * @param scorer used by the model definition.
   * @return a model definition using the specified settings.
   */
  def apply(name: String,
      version: String,
      extractor: Class[_ <: Extractor],
      scorer: Class[_ <: Scorer]): ModelDefinition = {
    new ModelDefinition(name, version, extractor, scorer, ModelDefinition.CURRENT_MODEL_DEF_VER)
  }

  /**
   * Creates a ModelDefinition given a JSON string. In the process, all fields are validated.
   *
   * @param json serialized model definition.
   * @return the validated model definition.
   */
  def fromJson(json: String): ModelDefinition = {
    // Parse the JSON into an Avro record.
    val avroModelDefinition: AvroModelDefinition = FromJson
        .fromJsonString(json, AvroModelDefinition.SCHEMA$)
        .asInstanceOf[AvroModelDefinition]
    val protocol = ProtocolVersion
        .parse(avroModelDefinition.getProtocolVersion)

    // Attempt to load the Extractor class.
    val extractor: Class[Extractor] = try {
      val checkClass = Class
          .forName(avroModelDefinition.getExtractorClass)
          .asInstanceOf[Class[Extractor]]
      checkClass.newInstance().asInstanceOf[Extractor]
      checkClass
    } catch {
      case _: ClassNotFoundException => {
        val extractorClass = avroModelDefinition.getExtractorClass
        val error = "The class \"%s\" could not be found.".format(extractorClass) +
            " Please ensure that you have provided a valid class name and that it is available" +
            " on your classpath."
        throw new ValidationException(error)
      }
      case _: ClassCastException => {
        val extractorClass = avroModelDefinition.getExtractorClass
        val error = "An instance of the class \"%s\" could not be cast".format(extractorClass) +
            " as an instance of Extractor. Please ensure that you have provided a valid class" +
            " that inherits from the Extractor class."
        throw new ValidationException(error)
      }
      case e @ (_ : IllegalAccessException | _ : InstantiationException |
          _ : ExceptionInInitializerError | _ : SecurityException) => {
        val error = "Unable to instantiate instance of Extractor class: "
        throw new ValidationException(error + e.toString)
      }
    }

    // Attempt to load the Scorer class.
    val scorer: Class[Scorer] = try {
      val checkClass = Class
          .forName(avroModelDefinition.getScorerClass)
          .asInstanceOf[Class[Scorer]]
      checkClass.newInstance().asInstanceOf[Scorer]
      checkClass
    } catch {
      case _: ClassNotFoundException => {
        val scorerClass = avroModelDefinition.getScorerClass
        val error = "The class \"%s\" could not be found.".format(scorerClass) +
            " Please ensure that you have provided a valid class name and that it is available" +
            " on your classpath."
        throw new ValidationException(error)
      }
      case _: ClassCastException => {
        val scorerClass = avroModelDefinition.getScorerClass
        val error = "An instance of the class \"%s\" could not be cast".format(scorerClass) +
            " as an instance of Scorer. Please ensure that you have provided a valid class" +
            " that inherits from the Scorer class."
        throw new ValidationException(error)
      }
      case e @ (_ : IllegalAccessException | _ : InstantiationException |
          _ : ExceptionInInitializerError | _ : SecurityException) => {
        val error = "Unable to instantiate instance of Scorer class: "
        throw new ValidationException(error + e.toString)
      }
    }

    // Build a model definition.
    new ModelDefinition(
        name = avroModelDefinition.getName,
        version = avroModelDefinition.getVersion,
        extractorClass = extractor,
        scorerClass = scorer,
        protocolVersion = protocol)
  }

  /**
   * Creates a ModelDefinition given a path in the local filesystem to a JSON file that
   * specifies a model. In the process, all fields are validated.
   *
   * @param path in the local filesystem to a JSON file containing a model definition.
   * @return the validated model definition.
   */
  def fromJsonFile(path: String): ModelDefinition = {
    val json: String = doAndClose(Source.fromFile(path)) { source: Source =>
      source.mkString
    }

    fromJson(json)
  }

  /**
   * Verifies that all fields in a model definition are valid. This validation method will
   * collect all validation errors into one exception.
   *
   * @param definition to validate.
   * @throws a ModelDefinitionValidationException if there are errors encountered while
   *     validating the provided model definition.
   */
  def validateModelDefinition(definition: ModelDefinition) {
    val extractorClass: Class[_] = definition.extractorClass
    val scorerClass: Class[_] = definition.scorerClass

    val validationErrors: Seq[Option[ValidationException]] = Seq(
        validateProtocolVersion(definition.protocolVersion),
        validateName(definition.name),
        validateVersion(definition.version)
    )

    val extractorFields: Set[String] = extractorOutputFieldNames(extractorClass)
    val fieldMappingErrors: Seq[Option[ValidationException]] =
      scorerInputFieldNames(scorerClass).map { inputFieldName: String =>
        validateScorerInputInExtractorOutputs(inputFieldName, extractorFields)
      }

    // Throw an exception if there were any validation errors.
    val allErrors = validationErrors ++ fieldMappingErrors
    val causes = allErrors.flatten
    if (!causes.isEmpty) {
      throw new ModelDefinitionValidationException(causes, VALIDATION_MESSAGE)
    }
  }

  /**
   * Verifies that a model definition's protocol version is supported.
   *
   * @param protocolVersion to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     protocol version.
   */
  private[express] def validateProtocolVersion(
      protocolVersion: ProtocolVersion): Option[ValidationException] = {
    if (MAX_MODEL_DEF_VER.compareTo(protocolVersion) < 0) {
      val error = "\"%s\" is the maximum protocol version supported. ".format(MAX_MODEL_DEF_VER) +
          "The provided model definition is of protocol version: \"%s\"".format(protocolVersion)
      Some(new ValidationException(error))
    } else if (MIN_MODEL_DEF_VER.compareTo(protocolVersion) > 0) {
      val error = "\"%s\" is the minimum protocol version supported. ".format(MIN_MODEL_DEF_VER) +
          "The provided model definition is of protocol version: \"%s\"".format(protocolVersion)
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Verifies that a model definition's name is valid.
   *
   * @param name to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     name of the model definition.
   */
  private[express] def validateName(name: String): Option[ValidationException] = {
    if (name.isEmpty) {
      val error = "The name of the model definition cannot be the empty string."
      Some(new ValidationException(error))
    } else if (!KijiNameValidator.isValidAlias(name)) {
      val error = "The name \"%s\" is not valid. Names must match the regex \"%s\"."
          .format(name, KijiNameValidator.VALID_ALIAS_PATTERN.pattern)
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Verifies that a model definition's version string is valid.
   *
   * @param version string to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     version string.
   */
  private[express] def validateVersion(version: String): Option[ValidationException] = {
    if (!version.matches(VERSION_REGEX)) {
      val error = "Model definition version strings must match the regex " +
          "\"%s\" (1.0.0 would be valid).".format(VERSION_REGEX)
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
  * Provides a set of the names of output fields used by an extractor class.
  * This is a helper function for validateScorerInputInExtractorOutputs.
  *
  * @param extractorClass from which to extract output fields.
  * @return a set of the extractor's output field names.
  */
  private[express] def extractorOutputFieldNames(extractorClass: Class[_]): Set[String] = {
    val extractor = extractorClass.newInstance()
    val extractorOutputFields: Fields = extractor
        .asInstanceOf[Extractor]
        .extractFn
        .fields
        ._2
    val extractorInputFields: Fields = extractor
        .asInstanceOf[Extractor]
        .extractFn
        .fields
        ._1

    if (!extractorOutputFields.isResults()) {
      Tuples
          .fieldsToSeq(extractorOutputFields)
          .toSet
    }
    else {
      // If Results is true, use the extractor's input fields as output.
      Tuples
          .fieldsToSeq(extractorInputFields)
          .toSet
    }
  }

  /**
  * Provides a set of the names of input fields used by a scorer class.
  * This is a helper function for validateScorerInputInExtractorOutputs.
  *
  * @param scorerClass from which to extract input fields.
  * @return a sequence of the scorer's input field names.
  */
  private[express] def scorerInputFieldNames(scorerClass: Class[_]): Seq[String] = {
    val scorer = scorerClass.newInstance()
    val scorerInputFields: Fields = scorer
        .asInstanceOf[Scorer]
        .scoreFn
        .fields

    Tuples
        .fieldsToSeq(scorerInputFields)
  }

  /**
   * Verifies that a scorer input field also exists in a list of extractor output fields.
   *
   * @param scorerInputFieldName to validate.
   * @param extractorOutputFieldNames to validate.
   * @return an optional ValidationException if there are errors encountered while validating that
   * a single scorer input field name exists among the extractor output field names.
   */
  private[express] def validateScorerInputInExtractorOutputs(
      scorerInputFieldName: String, extractorOutputFieldNames: Set[String]):
      Option[ValidationException] = {
    if (!extractorOutputFieldNames.contains(scorerInputFieldName)
        && !extractorOutputFieldNames.isEmpty) {
      Some(new ValidationException("Scorer's input field \'" + scorerInputFieldName +
          "\' does not match any extractor output fields."))
    } else {
      None
    }
  }
}
