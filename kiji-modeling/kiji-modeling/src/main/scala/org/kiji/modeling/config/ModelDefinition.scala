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

package org.kiji.modeling.config

import scala.io.Source

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.modeling.Evaluator
import org.kiji.modeling.Extractor
import org.kiji.modeling.Preparer
import org.kiji.modeling.Scorer
import org.kiji.modeling.Trainer
import org.kiji.modeling.avro.AvroModelDefinition
import org.kiji.modeling.framework.ModelConverters
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
 * val modelDefinition = ModelDefinition(
 *     name = "name",
 *     version = "1.0.0",
 *     preparerClass = Some(classOf[MyPreparer]),
 *     trainerClass = Some(classOf[MyTrainer]),
 *     evaluatorClass = Some(classOf[MyEvaluator]),
 *     scoreExtractorClass = Some(classOf[MyExtractor]),
 *     scorerClass = Some(classOf[MyScorer]))
 * }}}
 *
 * Alternatively a ModelDefinition can be created from JSON. JSON model specifications should be
 * written using the following format:
 * {{{
 * {
 *   "name": "name",
 *   "version": "1.0.0",
 *   "preparerClass": "org.kiji.modeling.config.MyPreparer",
 *   "trainerClass": "org.kiji.modeling.config.MyTrainer",
 *   "evaluatorClass": "org.kiji.modeling.config.MyEvaluator",
 *   "scorerClass": {
 *     "org.kiji.express.avro.AvroPhaseDefinition": {
 *       "extractor_class": null,
 *       "phase_class": "org.kiji.modeling.config.MyScorer"
 *     }
 *   },
 *   "protocol_version": "model_definition-0.2.0"
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
 * @param preparerClass to be used in the prepare phase of the model definition. Optional.
 * @param trainerClass to be used in the train phase of the model definition. Optional.
 * @param scoreExtractorClass that should be used for the score phase. Optional.
 * @param scorerClass to be used in the score phase of the model definition. Optional.
 * @param evaluatorClass to be used in the evaluate phase of the model defintion. Optional.
 * @param protocolVersion this model definition was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ModelDefinition(
    name: String,
    version: String,
    preparerClass: Option[java.lang.Class[_ <: Preparer]] = None,
    trainerClass: Option[java.lang.Class[_ <: Trainer]] = None,
    scoreExtractorClass: Option[java.lang.Class[_ <: Extractor]] = None,
    scorerClass: Option[java.lang.Class[_ <: Scorer]] = None,
    evaluatorClass: Option[java.lang.Class[_ <: Evaluator]] = None,
    private[kiji] val protocolVersion: ProtocolVersion =
        ModelDefinition.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model definition are valid.
  ModelDefinition.validateModelDefinition(this)

  /**
   * Serializes this model definition into a JSON string.
   *
   * @return a JSON string that represents the model definition.
   */
  def toJson: String = {
    val definition: AvroModelDefinition = ModelConverters.modelDefinitionToAvro(this)

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
   * @param preparerClass used by the model definition.
   * @param trainerClass used by the model definition.
   * @param scoreExtractorClass used by the model definition.
   * @param scorerClass used by the model definition.
   * @param evaluatorClass used by the model definition.
   * @return a new model definition using the settings specified to this method.
   */
  def withNewSettings(
      name: String = this.name,
      version: String = this.version,
      preparerClass: Option[Class[_ <: Preparer]] = this.preparerClass,
      trainerClass: Option[Class[_ <: Trainer]] = this.trainerClass,
      scoreExtractorClass: Option[Class[_ <: Extractor]] = this.scoreExtractorClass,
      scorerClass: Option[Class[_ <: Scorer]] = this.scorerClass,
      evaluatorClass: Option[Class[_ <: Evaluator]] = this.evaluatorClass): ModelDefinition = {
    new ModelDefinition(
        name,
        version,
        preparerClass,
        trainerClass,
        scoreExtractorClass,
        scorerClass,
        evaluatorClass,
        this.protocolVersion)
  }
}

/**
 * Companion object for ModelDefinition. Contains constants related to model definitions as well as
 * validation methods.
 */
object ModelDefinition {
  /** Maximum model definition version we can recognize. */
  val MAX_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.4.0")

  /** Minimum model definition version we can recognize. */
  val MIN_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.4.0")

  /** Current model definition protocol version. */
  val CURRENT_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.4.0")

  /** Regular expression used to validate a model definition version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model definition. */
  private[kiji] val VALIDATION_MESSAGE: String = "One or more errors occurred while " +
      "validating your model definition. Please correct the problems in your model definition " +
      "and try again."

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

    // Build a model definition.
    ModelConverters.modelDefinitionFromAvro(avroModelDefinition)
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
    val causes: Seq[ValidationException] =
        validateProtocolVersion(definition.protocolVersion).toSeq ++
        validateName(definition.name) ++
        validateVersion(definition.version) ++
        validatePhases(definition)

    // Throw an exception if there were any validation errors.
    if (!causes.isEmpty) {
      throw new ModelDefinitionValidationException(causes, VALIDATION_MESSAGE)
    }
  }

  /**
   * Verifies that a contiguous set of phases is defined.
   *
   * @param modelDefinition to validate.
   * @throws a ValidationException if an invalid combination of states is specified.
   */
  private[kiji] def validatePhases(
      modelDefinition: ModelDefinition): Seq[ValidationException] = {
    val noPhases =
        if (
            modelDefinition.preparerClass.isEmpty &&
            modelDefinition.trainerClass.isEmpty &&
            modelDefinition.scorerClass.isEmpty &&
            modelDefinition.evaluatorClass.isEmpty) {
          val error = "The model defines no phases. A valid definition requires at least one phase."
          Some(new ValidationException(error))
        } else if (
            modelDefinition.preparerClass.isDefined &&
            modelDefinition.trainerClass.isEmpty &&
            modelDefinition.scorerClass.isDefined) {
          val error = "Unsupported combination of phases. Prepare must be followed by train in " +
              "order to be useful to score."
          Some(new ValidationException(error))
        }  else if (
            modelDefinition.preparerClass.isDefined &&
            (modelDefinition.trainerClass.isEmpty ||
            modelDefinition.scorerClass.isEmpty) &&
            modelDefinition.evaluatorClass.isDefined) {
          val error = "Unsupported combination of phases. Prepare must be followed by train and " +
            "score in order to evaluate."
          Some(new ValidationException(error))
        } else if (
            modelDefinition.trainerClass.isDefined &&
            modelDefinition.scorerClass.isEmpty &&
            modelDefinition.evaluatorClass.isDefined) {
          val error = "Unsupported combination of phases. Train must be followed by score in " +
            "order to evaluate."
          Some(new ValidationException(error))
        } else {
          None
        }
    val badScore =
        if (
            modelDefinition.scoreExtractorClass.isDefined &&
            modelDefinition.scorerClass.isEmpty) {
          val error = "There is an extractor specified for a non-existent score phase."
          Some(new ValidationException(error))
        } else {
          None
        }

    noPhases.toSeq ++ badScore
  }

  /**
   * Verifies that a model definition's protocol version is supported.
   *
   * @param protocolVersion to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     protocol version.
   */
  private[kiji] def validateProtocolVersion(
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
  private[kiji] def validateName(name: String): Option[ValidationException] = {
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
  private[kiji] def validateVersion(version: String): Option[ValidationException] = {
    if (!version.matches(VERSION_REGEX)) {
      val error = "Model definition version strings must match the regex " +
          "\"%s\" (1.0.0 would be valid).".format(VERSION_REGEX)
      Some(new ValidationException(error))
    } else {
      None
    }
  }
}
