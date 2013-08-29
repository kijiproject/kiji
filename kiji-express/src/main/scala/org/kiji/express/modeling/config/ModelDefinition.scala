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

import scala.io.Source

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.avro.AvroModelDefinition
import org.kiji.express.avro.AvroPhaseSpec
import org.kiji.express.modeling.Extractor
import org.kiji.express.modeling.Preparer
import org.kiji.express.modeling.Scorer
import org.kiji.express.modeling.Trainer
import org.kiji.express.util.Resources.doAndClose
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
 *   name = "name",
 *   version = "1.0.0",
 *   preparer = Some(classOf[MyPreparer]),
 *   trainer = Some(classOf[MyTrainer]),
 *   scoreExtractor = Some(classOf[MyExtractor]),
 *   scorer = Some(classOf[MyScorer])
 * )
 * }}}
 *
 * Alternatively a ModelDefinition can be created from JSON. JSON model specifications should be
 * written using the following format:
 * {{{
 * {
 *  "name":"name",
 *  "version":"1.0.0",
 *  "preparer": "org.kiji.express.modeling.config.MyPreparer",
 *  "trainer": "org.kiji.express.modeling.config.MyTrainer",
 *  "scorer":{
 *    "org.kiji.express.avro.AvroPhaseSpec":{
 *      "extractor_class":null,
 *      "phase_class":"org.kiji.express.modeling.config.MyScorer"
 *    }
 *  },
 *  "protocol_version":"model_definition-0.2.0"
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
 * @param scoreExtractor is the extractor class that should be used for the score phase. Optional.
 * @param scorerClass to be used in the score phase of the model definition.
 * @param protocolVersion this model definition was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ModelDefinition private[express] (
    val name: String,
    val version: String,
    val preparerClass: Option[java.lang.Class[_ <: Preparer]],
    val trainerClass: Option[java.lang.Class[_ <: Trainer]],
    val scoreExtractor: Option[java.lang.Class[_ <: Extractor]],
    val scorerClass: Option[java.lang.Class[_ <: Scorer]],
    private[express] val protocolVersion: ProtocolVersion =
        ModelDefinition.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model definition are valid.
  ModelDefinition.validateModelDefinition(this)

  /**
   * Serializes this model definition into a JSON string.
   *
   * @return a JSON string that represents the model definition.
   */
  def toJson(): String = {
    // Build an AvroModelDefinition record.
    val avroPreparer = preparerClass.map(_.getName).getOrElse(null)

    val avroTrainer = trainerClass.map(_.getName).getOrElse(null)

    val avroScorer = scorerClass
      .map { sclass =>
        AvroPhaseSpec
            .newBuilder()
            .setExtractorClass(scoreExtractor.map {_.getName} .getOrElse(null))
            .setPhaseClass(sclass.getName)
            .build()
      }
      .getOrElse(null)

    // scalastyle:off null
    val definition: AvroModelDefinition = AvroModelDefinition
        .newBuilder()
        .setName(name)
        .setVersion(version)
        .setProtocolVersion(protocolVersion.toString)
        .setPreparer(avroPreparer)
        .setTrainer(avroTrainer)
        .setScorer(avroScorer)
        .build()
    // scalastyle:on null

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
   * @param preparer used by the model definition.
   * @param trainer used by the model definition.
   * @param scoreExtractor used by the model definition.
   * @param scorer used by the model definition.
   * @return a new model definition using the settings specified to this method.
   */
  def withNewSettings(
      name: String = this.name,
      version: String = this.version,
      preparer: Option[Class[_ <: Preparer]] = this.preparerClass,
      trainer: Option[Class[_ <: Trainer]] = this.trainerClass,
      scoreExtractor: Option[Class[_ <: Extractor]] = this.scoreExtractor,
      scorer: Option[Class[_ <: Scorer]] = this.scorerClass): ModelDefinition = {
    new ModelDefinition(
        name,
        version,
        preparer,
        trainer,
        scoreExtractor,
        scorer,
        this.protocolVersion)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case definition: ModelDefinition => {
        name == definition.name &&
            version == definition.version &&
            preparerClass == definition.preparerClass &&
            trainerClass == definition.trainerClass &&
            scoreExtractor == definition.scoreExtractor &&
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
          preparerClass,
          trainerClass,
          scoreExtractor,
          scorerClass,
          protocolVersion)
}

/**
 * Companion object for ModelDefinition. Contains constants related to model definitions as well as
 * validation methods.
 */
object ModelDefinition {
  /** Maximum model definition version we can recognize. */
  val MAX_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.2.0")

  /** Minimum model definition version we can recognize. */
  val MIN_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.2.0")

  /** Current model definition protocol version. */
  val CURRENT_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_definition-0.2.0")

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
   * @param preparer used by the model definition.
   * @param trainer used by the model definition.
   * @param scoreExtractor used by the model definition.
   * @param scorer used by the model definition.
   * @return a model definition using the specified settings.
   */
  def apply(name: String,
      version: String,
      preparer: Option[Class[_ <: Preparer]] = None,
      trainer: Option[Class[_ <: Trainer]] = None,
      scoreExtractor: Option[Class[_ <: Extractor]] = None,
      scorer: Option[Class[_ <: Scorer]] = None): ModelDefinition = {
    new ModelDefinition(
        name = name,
        version = version,
        preparerClass = preparer,
        trainerClass = trainer,
        scoreExtractor = scoreExtractor,
        scorerClass = scorer,
        protocolVersion = ModelDefinition.CURRENT_MODEL_DEF_VER)
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

    /**
     * Retrieves the class for the provided phase implementation class name handling errors
     * properly.
     *
     * @param phaseImplName to build phase class from.
     * @param phase that the resulting class should belong to.
     * @tparam T is the type of the phase class.
     * @return the phase implementation class.
     */
    def getClassForPhase[T](phaseImplName: String, phase: Class[T]): Class[T] = {
      val checkClass: Class[T] = try {
        Class.forName(phaseImplName).asInstanceOf[Class[T]]
      } catch {
        case _: ClassNotFoundException => {
          val error = "The class \"%s\" could not be found.".format(phaseImplName) +
              " Please ensure that you have provided a valid class name and that it is available" +
              " on your classpath."
          throw new ValidationException(error)
        }
      }

      // Ensure that the class can be instantiated (force an early failure).
      try {
        if (!phase.isInstance(checkClass.newInstance())) {
          val error = ("An instance of the class \"%s\" could not be cast as an instance of %s." +
              " Please ensure that you have provided a valid class that inherits from the" +
              " %s class.").format(phaseImplName, phase.getSimpleName, phase.getSimpleName)
          throw new ValidationException(error)
        }
      } catch {
        case e @ (_ : IllegalAccessException | _ : InstantiationException |
                  _ : ExceptionInInitializerError | _ : SecurityException) => {
          val error = "Unable to create instance of %s.".format(checkClass.getCanonicalName)
          throw new ValidationException(error + e.toString)
        }
      }

      checkClass
    }

    // Attempt to load the Preparer class and corresponding Extractor.
    val preparerClassName: Option[String] = Option(avroModelDefinition.getPreparer)
    val preparer: Option[Class[Preparer]] = preparerClassName.map { className: String =>
      getClassForPhase[Preparer](
          phaseImplName = className,
          phase = classOf[Preparer])
    }

    // Attempt to load the Trainer class and corresponding Extractor.
    val trainerClassName: Option[String] = Option(avroModelDefinition.getTrainer)
    val trainer: Option[Class[Trainer]] = trainerClassName.map { className: String =>
      getClassForPhase[Trainer](
          phaseImplName = className,
          phase = classOf[Trainer])
    }

    // Attempt to load the Scorer class and corresponding Extractor.
    val avroScorerOption = Option(avroModelDefinition.getScorer)
    val scorerClassName: Option[String] = avroScorerOption.map(_.getPhaseClass)
    val scorer: Option[Class[Scorer]] = scorerClassName.map { className: String =>
      getClassForPhase[Scorer](
          phaseImplName = className,
          phase = classOf[Scorer])
    }

    val scoreExtractorClassName: Option[String] = avroScorerOption match {
      case None => None
      case Some(phaseSpec) => Option(phaseSpec.getExtractorClass)
    }
    val scoreExtractor: Option[Class[Extractor]] = scoreExtractorClassName
      .map { className: String => getClassForPhase[Extractor](
        phaseImplName = className,
        phase = classOf[Extractor])
    }

    // Build a model definition.
    new ModelDefinition(
        name = avroModelDefinition.getName,
        version = avroModelDefinition.getVersion,
        preparerClass = preparer,
        trainerClass = trainer,
        scoreExtractor = scoreExtractor,
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
    val scorerClass: Option[Class[_]] = definition.scorerClass

    val validationErrors: Seq[Option[ValidationException]] = Seq(
        validateProtocolVersion(definition.protocolVersion),
        validateName(definition.name),
        validateVersion(definition.version)
    ) ++ validatePhases(definition)

    // Throw an exception if there were any validation errors.
    val causes = validationErrors.flatten
    if (!causes.isEmpty) {
      throw new ModelDefinitionValidationException(causes, VALIDATION_MESSAGE)
    }
  }

  private[express] def validatePhases(modelDefinition: ModelDefinition)
      : Seq[Option[ValidationException]] = {
    val noPhases = if (modelDefinition.preparerClass.isEmpty &&
        modelDefinition.trainerClass.isEmpty &&
        modelDefinition.scorerClass.isEmpty) {
      val error = "The model defines no phases. A valid definition requires at least one phase."
      Some(new ValidationException(error))
    } else if (modelDefinition.preparerClass.isDefined &&
        modelDefinition.trainerClass.isEmpty &&
        modelDefinition.scorerClass.isDefined) {
      Some(new ValidationException("Unsupported combination of phases. Prepare must be followed by "
        + "train in order to be useful in to score."))

    } else {
      None
    }

    val badScore = if (modelDefinition.scoreExtractor.isDefined
        && modelDefinition.scorerClass.isEmpty) {
      val error = "There is an extractor specified for a non-existent score phase."
      Some(new ValidationException(error))
    } else {
      None
    }
    Seq(noPhases, badScore)
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
}
