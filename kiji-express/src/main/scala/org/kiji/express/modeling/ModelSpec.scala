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

package org.kiji.express.modeling

import scala.io.Source

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.Resources.doAndClose
import org.kiji.express.avro.AvroModelSpec
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.KijiNameValidator
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson

/**
 * A ModelSpec is a descriptor of the computational logic to use at different phases of
 * of a modeling workflow.
 *
 * Currently this class can only be created from a model specification in a JSON file. JSON model
 * specifications should be written using the following format:
 * {{{
 * {
 *   "name" : "identifier-for-this-model",
 *   "version" : "1.0.0",
 *   "extractor_class" : "com.organization.YourExtractor",
 *   "scorer_class" : "com.organization.YourScorer",
 *   "protocol_version" : "model_spec-0.1.0"
 * }
 * }}}
 *
 * @param name of the model spec.
 * @param version of the model spec.
 * @param extractorClass to be used in the extract phase of the model specification.
 * @param scorerClass to be used in the score phase of the model specification.
 * @param protocolVersion this model specification was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ModelSpec private[express] (
    val name: String,
    val version: String,
    val extractorClass: java.lang.Class[_ <: Extractor],
    val scorerClass: java.lang.Class[_ <: Scorer],
    private[express] val protocolVersion: ProtocolVersion = ModelSpec.CURRENT_MODEL_SPEC_VER) {
  // Ensure that all fields set for this model spec are valid.
  ModelSpec.validateModelSpec(this)

  /**
   * Serializes this model specification into a JSON string.
   *
   * @return a JSON string that represents the model spec.
   */
  final def toJson(): String = {
    // Build an AvroModelSpec record.
    val spec: AvroModelSpec = AvroModelSpec
        .newBuilder()
        .setName(name)
        .setVersion(version)
        .setProtocolVersion(protocolVersion.toString)
        .setExtractorClass(extractorClass.getName)
        .setScorerClass(scorerClass.getName)
        .build()

    // Encode it into JSON.
    ToJson.toAvroJsonString(spec)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case spec: ModelSpec => {
        name == spec.name &&
            version == spec.version &&
            extractorClass == spec.extractorClass &&
            scorerClass == spec.scorerClass &&
            protocolVersion == spec.protocolVersion
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(name, version, extractorClass, scorerClass, protocolVersion)
}

/**
 * Companion object for ModelSpec. Contains constants related to model specifications as well as
 * validation methods.
 */
object ModelSpec {
  /** Maximum ModelSpec version we can recognize. */
  val MAX_MODEL_SPEC_VER: ProtocolVersion = ProtocolVersion.parse("model_spec-0.1.0")

  /** Minimum ModelSpec version we can recognize. */
  val MIN_MODEL_SPEC_VER: ProtocolVersion = ProtocolVersion.parse("model_spec-0.1.0")

  /** Current ModelSpec protocol version. */
  val CURRENT_MODEL_SPEC_VER: ProtocolVersion = ProtocolVersion.parse("model_spec-0.1.0")

  /** Regular expression used to validate a model spec version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model specification. */
  private[express] val VALIDATION_MESSAGE = "One or more errors occurred while validating your " +
      "model specification. Please correct the problems in your model specification and try again."

  /**
   * Creates a ModelSpec given a JSON string. In the process, all fields are validated.
   *
   * @param json specification of a model.
   * @return the validated model specification.
   */
  def fromJson(json: String): ModelSpec = {
    // Parse the JSON into an Avro record.
    val avroModelSpec: AvroModelSpec = FromJson
        .fromJsonString(json, AvroModelSpec.SCHEMA$)
        .asInstanceOf[AvroModelSpec]
    val protocol = ProtocolVersion
        .parse(avroModelSpec.getProtocolVersion)

    // Attempt to load the Extractor class.
    val extractor = try {
      Class
          .forName(avroModelSpec.getExtractorClass)
          .asInstanceOf[Class[Extractor]]
    } catch {
      case _: ClassNotFoundException => {
        val error = "The class \"%s\" could not be found.".format(avroModelSpec.getExtractorClass) +
            " Please ensure that you have provided a valid class name and that it is available " +
            "on your classpath."
        throw new ValidationException(error)
      }
    }

    // Attempt to load the Scorer class.
    val scorer = try {
      Class
          .forName(avroModelSpec.getScorerClass)
          .asInstanceOf[Class[Scorer]]
    } catch {
      case _: ClassNotFoundException => {
        val error = "The class \"%s\" could not be found.".format(avroModelSpec.getScorerClass) +
            " Please ensure that you have provided a valid class name and that it is available " +
            "on your classpath."
        throw new ValidationException(error)
      }
    }

    // Build a model spec.
    new ModelSpec(
        name = avroModelSpec.getName,
        version = avroModelSpec.getVersion,
        extractorClass = extractor,
        scorerClass = scorer,
        protocolVersion = protocol)
  }

  /**
   * Creates a ModelSpec given a path in the local filesystem to a JSON file that
   * specifies a model. In the process, all fields are validated.
   *
   * @param path in the local filesystem to a JSON specification of a model spec.
   * @return the validated model specification.
   */
  def fromJsonFile(path: String): ModelSpec = {
    val json: String = doAndClose(Source.fromFile(path)) { source: Source =>
      source.mkString
    }

    fromJson(json)
  }

  /**
   * Runs a block of code catching any validation exceptions that occur.
   *
   * @param fn to run.
   * @return an exception if an error was thrown.
   */
  private def catchError(fn: => Unit): Option[ValidationException] = {
    try {
      fn
      None
    } catch {
      case validationError: ValidationException => Some(validationError)
    }
  }

  /**
   * Verifies that all fields in a model specification are valid. This validation method will
   * collect all validation errors into one exception.
   *
   * @param spec to validate.
   * @throws ModelSpecValidationException if there are errors encountered while validating the
   *     provided model spec.
   */
  def validateModelSpec(spec: ModelSpec) {
    val extractorClass = Option(spec.extractorClass)
    val scorerClass = Option(spec.scorerClass)

    // Collect errors from the other validation steps.
    val errors: Seq[Option[ValidationException]] = Seq(
        catchError(validateProtocolVersion(spec.protocolVersion)),
        catchError(validateName(spec.name)),
        catchError(validateVersion(spec.version)),
        extractorClass
            .flatMap { x => catchError(validateExtractorClass(x)) },
        scorerClass
            .flatMap { x => catchError(validateScorerClass(x)) })

    // Throw an exception if there were any validation errors.
    val causes = errors.flatten
    if (!causes.isEmpty) {
      throw new ModelSpecValidationException(causes, VALIDATION_MESSAGE)
    }
  }

  /**
   * Verifies that a model specification's protocol version is supported.
   *
   * @param protocolVersion to validate.
   */
  def validateProtocolVersion(protocolVersion: ProtocolVersion) {
    if (MAX_MODEL_SPEC_VER.compareTo(protocolVersion) < 0) {
      val error = "\"%s\" is the maximum protocol version supported. ".format(MAX_MODEL_SPEC_VER) +
          "The provided model spec is of protocol version: \"%s\"".format(protocolVersion)

      throw new ValidationException(error)
    } else if (MIN_MODEL_SPEC_VER.compareTo(protocolVersion) > 0) {
      val error = "\"%s\" is the minimum protocol version supported. ".format(MIN_MODEL_SPEC_VER) +
          "The provided model spec is of protocol version: \"%s\"".format(protocolVersion)

      throw new ValidationException(error)
    }
  }

  /**
   * Verifies that a model specification's name is valid.
   *
   * @param name to validate.
   */
  def validateName(name: String) {
    if (name.isEmpty) {
      throw new ValidationException("The name of the model spec cannot be the empty string.")
    } else if (!KijiNameValidator.isValidAlias(name)) {
      throw new ValidationException("The name \"%s\" is not valid. ".format(name) +
          "Names must match the regex \"%s\"."
              .format(KijiNameValidator.VALID_ALIAS_PATTERN.pattern))
    }
  }

  /**
   * Verifies that a model specification's version string is valid.
   *
   * @param version string to validate.
   */
  def validateVersion(version: String) {
    if (!version.matches(VERSION_REGEX)) {
      val error = "Model spec version strings must match the regex \"%s\" (1.0.0 would be valid)."
          .format(VERSION_REGEX)
      throw new ValidationException(error)
    }
  }

  /**
   * Verifies that a model specification's extractor class is a valid class to use during the
   * extract phase.
   *
   * @param extractorClass to validate.
   */
  def validateExtractorClass(extractorClass: Class[_]) {
    if (!classOf[Extractor].isAssignableFrom(extractorClass)) {
      val error = "The class \"%s\" does not implement the Extractor trait."
          .format(extractorClass.getName)
      throw new ValidationException(error)
    }
  }

  /**
   * Verifies that a model specification's scorer class is a valid class to use during the score
   * phase.
   *
   * @param scorerClass to validate.
   */
  def validateScorerClass(scorerClass: Class[_]) {
    if (!classOf[Scorer].isAssignableFrom(scorerClass)) {
      val error = "The class \"%s\" does not implement the Scorer trait."
          .format(scorerClass.getName)
      throw new ValidationException(error)
    }
  }
}
