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

package org.kiji.modeling.framework

import scala.collection.JavaConverters._

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import org.kiji.express.flow.AndFilterSpec
import org.kiji.express.flow.Between
import org.kiji.express.flow.ColumnFamilyInputSpec
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.ColumnRangeFilterSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.ColumnFilterSpec
import org.kiji.express.flow.OrFilterSpec
import org.kiji.express.flow.PagingSpec
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.RegexQualifierFilterSpec
import org.kiji.express.flow.SchemaSpec
import org.kiji.express.flow.TimeRange
import org.kiji.modeling.Evaluator
import org.kiji.modeling.Extractor
import org.kiji.modeling.Preparer
import org.kiji.modeling.Scorer
import org.kiji.modeling.Trainer
import org.kiji.modeling.avro.AvroColumnFamilyInputSpec
import org.kiji.modeling.avro.AvroColumnFamilyOutputSpec
import org.kiji.modeling.avro.AvroColumnRangeFilter
import org.kiji.modeling.avro.AvroEvaluateEnvironment
import org.kiji.modeling.avro.AvroFilter
import org.kiji.modeling.avro.AvroInputFieldBinding
import org.kiji.modeling.avro.AvroInputSpec
import org.kiji.modeling.avro.AvroKeyValueStoreSpec
import org.kiji.modeling.avro.AvroKeyValueStoreType
import org.kiji.modeling.avro.AvroKijiInputSpec
import org.kiji.modeling.avro.AvroKijiOutputSpec
import org.kiji.modeling.avro.AvroKijiSingleColumnOutputSpec
import org.kiji.modeling.avro.AvroModelDefinition
import org.kiji.modeling.avro.AvroModelEnvironment
import org.kiji.modeling.avro.AvroOutputFieldBinding
import org.kiji.modeling.avro.AvroOutputSpec
import org.kiji.modeling.avro.AvroPhaseDefinition
import org.kiji.modeling.avro.AvroPrepareEnvironment
import org.kiji.modeling.avro.AvroProperty
import org.kiji.modeling.avro.AvroQualifiedColumnInputSpec
import org.kiji.modeling.avro.AvroQualifiedColumnOutputSpec
import org.kiji.modeling.avro.AvroRegexQualifierFilter
import org.kiji.modeling.avro.AvroSchemaSpec
import org.kiji.modeling.avro.AvroSpecificSchemaSpec
import org.kiji.modeling.avro.AvroScoreEnvironment
import org.kiji.modeling.avro.AvroSequenceFileSourceSpec
import org.kiji.modeling.avro.AvroTextSourceSpec
import org.kiji.modeling.avro.AvroTimeRange
import org.kiji.modeling.avro.AvroTrainEnvironment
import org.kiji.modeling.config.EvaluateEnvironment
import org.kiji.modeling.config.InputSpec
import org.kiji.modeling.config.KeyValueStoreSpec
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiOutputSpec
import org.kiji.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.OutputSpec
import org.kiji.modeling.config.PrepareEnvironment
import org.kiji.modeling.config.ScoreEnvironment
import org.kiji.modeling.config.SequenceFileSourceSpec
import org.kiji.modeling.config.TextSourceSpec
import org.kiji.modeling.config.TrainEnvironment
import org.kiji.modeling.config.ValidationException
import org.kiji.schema.util.ProtocolVersion

/**
 * Object containing code for converting Avro records used by the model lifecycle to their scala
 * case class counterparts.
 */
object ModelConverters {
  /**
   * Builds a model definition from its avro record representation.
   *
   * @param modelDefinition to build from.
   * @return a populated model definition.
   */
  def modelDefinitionFromAvro(modelDefinition: AvroModelDefinition): ModelDefinition = {
    val protocolVersion = ProtocolVersion
        .parse(modelDefinition.getProtocolVersion)

    // Attempt to load the Preparer class.
    val preparerClass: Option[Class[Preparer]] = Option(modelDefinition.getPreparerClass)
        .map { className: String =>
          getClassForPhase[Preparer](
              phaseImplName = className,
              phase = classOf[Preparer])
        }

    // Attempt to load the Trainer class.
    val trainerClass: Option[Class[Trainer]] = Option(modelDefinition.getTrainerClass)
        .map { className: String =>
          getClassForPhase[Trainer](
              phaseImplName = className,
              phase = classOf[Trainer])
        }

    // Attempt to load the Scorer class and corresponding Extractor.
    val avroScorerPhase: Option[AvroPhaseDefinition] = Option(modelDefinition.getScorerPhase)
    val scorerClass: Option[Class[Scorer]] = avroScorerPhase
        .map { phaseDefinition =>
          getClassForPhase[Scorer](
              phaseImplName = phaseDefinition.getPhaseClass,
              phase = classOf[Scorer])
        }
    val scoreExtractorClass: Option[Class[Extractor]] = avroScorerPhase
        .flatMap { phaseDefinition => Option(phaseDefinition.getExtractorClass) }
        .map { className: String =>
          getClassForPhase[Extractor](
              phaseImplName = className,
              phase = classOf[Extractor])
        }

    // Attempt to load the Evaluator class.
    val evaluatorClass: Option[Class[Evaluator]] = Option(modelDefinition.getEvaluatorClass)
        .map { className: String =>
          getClassForPhase[Evaluator](
              phaseImplName = className,
              phase = classOf[Evaluator])
        }

    // Build a model definition.
    new ModelDefinition(
        name = modelDefinition.getName,
        version = modelDefinition.getVersion,
        preparerClass = preparerClass,
        trainerClass = trainerClass,
        scoreExtractorClass = scoreExtractorClass,
        scorerClass = scorerClass,
        evaluatorClass = evaluatorClass,
        protocolVersion = protocolVersion)
  }

  /**
   * Converts a model definition to its avro record representation.
   *
   * @param modelDefinition to convert.
   * @return an avro record.
   */
  def modelDefinitionToAvro(modelDefinition: ModelDefinition): AvroModelDefinition = {
    // Build the Prepare phase's definition.
    val avroPreparerClass = modelDefinition
        .preparerClass
        .map { _.getName }
        .getOrElse(null)

    // Build the Train phase's definition.
    val avroTrainerClass = modelDefinition
        .trainerClass
        .map { _.getName }
        .getOrElse(null)

    // Build the Score phase's definition.
    val avroScorerClass = phaseDefinitionToAvro(
        phaseClass = modelDefinition.scorerClass,
        extractorClass = modelDefinition.scoreExtractorClass)

    // Build the Evaluator phase's definition.
    val avroEvaluatorClass = modelDefinition
        .evaluatorClass
        .map { _.getName }
        .getOrElse(null)

    // Build the model definition.
    AvroModelDefinition
        .newBuilder()
        .setName(modelDefinition.name)
        .setVersion(modelDefinition.version)
        .setProtocolVersion(modelDefinition.protocolVersion.toString)
        .setPreparerClass(avroPreparerClass)
        .setTrainerClass(avroTrainerClass)
        .setScorerPhase(avroScorerClass)
        .setEvaluatorClass(avroEvaluatorClass)
        .build()
  }

  /**
   * Builds a model environment from its avro record representation.
   *
   * @param environment to build from.
   * @return a populated model environment.
   */
  def modelEnvironmentFromAvro(environment: AvroModelEnvironment): ModelEnvironment = {
    val protocol = ProtocolVersion.parse(environment.getProtocolVersion)

    // Load the model's phase environments.
    val prepareEnvironment = Option(environment.getPrepareEnvironment)
        .map { prepareEnvironmentFromAvro }
    val trainEnvironment = Option(environment.getTrainEnvironment)
        .map { trainEnvironmentFromAvro }
    val scoreEnvironment = Option(environment.getScoreEnvironment)
        .map { scoreEnvironmentFromAvro }
    val evaluateEnvironment = Option(environment.getEvaluateEnvironment)
        .map { evaluateEnvironmentFromAvro }

    // Build a model environment.
    new ModelEnvironment(
        name = environment.getName,
        version = environment.getVersion,
        prepareEnvironment = prepareEnvironment,
        trainEnvironment = trainEnvironment,
        scoreEnvironment = scoreEnvironment,
        evaluateEnvironment = evaluateEnvironment,
        protocolVersion = protocol)
  }

  /**
   * Converts a model environment to its avro record representation.
   *
   * @param environment to convert.
   * @return an avro record.
   */
  def modelEnvironmentToAvro(environment: ModelEnvironment): AvroModelEnvironment = {
    // Build an AvroPrepareEnvironment record.
    val avroPrepareEnvironment: Option[AvroPrepareEnvironment] = environment
        .prepareEnvironment
        .map { prepareEnvironmentToAvro }

    // Build an AvroTrainEnvironment record.
    val avroTrainEnvironment: Option[AvroTrainEnvironment] = environment
        .trainEnvironment
        .map { trainEnvironmentToAvro }

    // Build an AvroScoreEnvironment record.
    val avroScoreEnvironment: Option[AvroScoreEnvironment] = environment
        .scoreEnvironment
        .map { scoreEnvironmentToAvro }

    // Build an AvroEvaluateEnvironment record.
    val avroEvaluateEnvironment: Option[AvroEvaluateEnvironment] = environment
        .evaluateEnvironment
        .map { evaluateEnvironmentToAvro }

    // Build an AvroModelEnvironment record.
    AvroModelEnvironment
        .newBuilder()
        .setName(environment.name)
        .setVersion(environment.version)
        .setProtocolVersion(environment.protocolVersion.toString)
        .setPrepareEnvironment(avroPrepareEnvironment.getOrElse(null))
        .setTrainEnvironment(avroTrainEnvironment.getOrElse(null))
        .setScoreEnvironment(avroScoreEnvironment.getOrElse(null))
        .setEvaluateEnvironment(avroEvaluateEnvironment.getOrElse(null))
        .build()
  }

  /**
   * Builds a prepare environment from its avro record representation.
   *
   * @param environment to build from.
   * @return a populated prepare environment.
   */
  def prepareEnvironmentFromAvro(environment: AvroPrepareEnvironment): PrepareEnvironment = {
    new PrepareEnvironment(
        inputSpec = inputSpecsFromAvro(environment.getInputSpec),
        outputSpec = outputSpecsFromAvro(environment.getOutputSpec),
        keyValueStoreSpecs = environment
            .getKvStores
            .asScala
            .map { keyValueStoreSpecFromAvro })
  }

  /**
   * Converts a prepare environment to its avro record representation.
   *
   * @param environment to convert.
   * @return an avro record.
   */
  def prepareEnvironmentToAvro(environment: PrepareEnvironment): AvroPrepareEnvironment = {
    AvroPrepareEnvironment
        .newBuilder()
        .setInputSpec(inputSpecsToAvro(environment.inputSpec))
        .setOutputSpec(outputSpecsToAvro(environment.outputSpec))
        .setKvStores(environment.keyValueStoreSpecs.map { keyValueStoreSpecToAvro } .asJava)
        .build()
  }

  /**
   * Builds a train environment from its avro record representation.
   *
   * @param environment to build from.
   * @return a populated train environment.
   */
  def trainEnvironmentFromAvro(environment: AvroTrainEnvironment): TrainEnvironment = {
    new TrainEnvironment(
        inputSpec = inputSpecsFromAvro(environment.getInputSpec),
        outputSpec = outputSpecsFromAvro(environment.getOutputSpec),
        keyValueStoreSpecs = environment
            .getKvStores
            .asScala
            .map { keyValueStoreSpecFromAvro })
  }

  /**
   * Converts a train environment to its avro record representation.
   *
   * @param environment to convert.
   * @return an avro record.
   */
  def trainEnvironmentToAvro(environment: TrainEnvironment): AvroTrainEnvironment = {
    AvroTrainEnvironment
        .newBuilder()
        .setInputSpec(inputSpecsToAvro(environment.inputSpec))
        .setOutputSpec(outputSpecsToAvro(environment.outputSpec))
        .setKvStores(environment.keyValueStoreSpecs.map { keyValueStoreSpecToAvro } .asJava)
        .build()
  }

  /**
   * Builds a score environment from its avro record representation.
   *
   * @param environment to build from.
   * @return a populated score environment.
   */
  def scoreEnvironmentFromAvro(environment: AvroScoreEnvironment): ScoreEnvironment = {
    val inputSpec: KijiInputSpec = kijiInputSpecFromAvro(environment.getInputSpec)
    val outputSpec: KijiSingleColumnOutputSpec =
      kijiSingleColumnOutputSpecFromAvro(environment.getOutputSpec)
    new ScoreEnvironment(
        inputSpec = inputSpec,
        outputSpec = outputSpec,
        keyValueStoreSpecs = environment
            .getKvStores
            .asScala
            .map { keyValueStoreSpecFromAvro })
  }

  /**
   * Converts a score environment to its avro record representation.
   *
   * @param environment to convert.
   * @return an avro record.
   */
  def scoreEnvironmentToAvro(environment: ScoreEnvironment): AvroScoreEnvironment = {
    val avroInputSpec: AvroKijiInputSpec = kijiInputSpecToAvro(environment.inputSpec)
    val avroOutputSpec: AvroKijiSingleColumnOutputSpec =
        kijiSingleColumnOutputSpecToAvro(environment.outputSpec)
    AvroScoreEnvironment
        .newBuilder()
        .setInputSpec(avroInputSpec)
        .setOutputSpec(avroOutputSpec)
        .setKvStores(environment.keyValueStoreSpecs.map { keyValueStoreSpecToAvro } .asJava)
        .build()
  }

  /**
   * Builds an evaluate environment from its avro record representation.
   *
   * @param environment to build from.
   * @return a populated evaluate environment.
   */
  def evaluateEnvironmentFromAvro(environment: AvroEvaluateEnvironment): EvaluateEnvironment = {
    val inputSpec: KijiInputSpec = kijiInputSpecFromAvro(environment.getInputSpec)

    new EvaluateEnvironment(
      inputSpec = inputSpec,
      outputSpec = outputSpecFromAvro(environment.getOutputSpec),
      keyValueStoreSpecs = environment
        .getKvStores
        .asScala
        .map { keyValueStoreSpecFromAvro })
  }

  /**
   * Converts an evaluate environment to its avro record representation.
   *
   * @param environment to convert.
   * @return an avro record.
   */
  def evaluateEnvironmentToAvro(environment: EvaluateEnvironment): AvroEvaluateEnvironment = {
    val avroInputSpec: AvroKijiInputSpec = kijiInputSpecToAvro(environment.inputSpec)

    AvroEvaluateEnvironment
      .newBuilder()
      .setInputSpec(avroInputSpec)
      .setOutputSpec(outputSpecToAvro(environment.outputSpec))
      .setKvStores(environment.keyValueStoreSpecs.map { keyValueStoreSpecToAvro } .asJava)
      .build()
  }

  /**
   * Builds a keyValueStoreSpec specification from its avro record representation.
   *
   * @param keyValueStoreSpec to build from.
   * @return a populated keyValueStoreSpec specification.
   */
  def keyValueStoreSpecFromAvro(keyValueStoreSpec: AvroKeyValueStoreSpec): KeyValueStoreSpec = {
    KeyValueStoreSpec(
        storeType = keyValueStoreSpec.getStoreType.name,
        name = keyValueStoreSpec.getName,
        properties = keyValueStoreSpec
            .getProperties
            .asScala
            .map { prop => (prop.getName, prop.getValue) }
            .toMap)
  }

  /**
   * Converts a keyValueStoreSpec specification to its avro record representation.
   *
   * @param keyValueStoreSpec to convert.
   * @return an avro record.
   */
  def keyValueStoreSpecToAvro(keyValueStoreSpec: KeyValueStoreSpec): AvroKeyValueStoreSpec = {
    val avroProperties: java.util.List[AvroProperty] = keyValueStoreSpec
        .properties
        .map { case (name, value) => new AvroProperty(name, value) }
        .toSeq
        .asJava

    AvroKeyValueStoreSpec
        .newBuilder()
        .setStoreType(AvroKeyValueStoreType.valueOf(keyValueStoreSpec.storeType))
        .setName(keyValueStoreSpec.name)
        .setProperties(avroProperties)
        .build()
  }

  /**
   * Retrieves the class for the provided phase implementation class name handling errors
   * properly.
   *
   * @param phaseImplName to build phase class from.
   * @param phase that the resulting class should belong to.
   * @tparam T is the type of the phase class.
   * @return the phase implementation class.
   */
  private[kiji] def getClassForPhase[T](phaseImplName: String, phase: Class[T]): Class[T] = {
    val checkClass: Class[T] = try {
      new java.lang.Thread()
          .getContextClassLoader
          .loadClass(phaseImplName)
          .asInstanceOf[Class[T]]
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

  /**
   * Builds an Avro phase definition record from the provided phase class and extractor class.
   *
   * @param phaseClass to pack in the resulting Avro record.
   * @param extractorClass to pack in the resulting Avro record.
   * @return An Avro phase definition record.
   */
  private[kiji] def phaseDefinitionToAvro(
      phaseClass: Option[Class[_]],
      extractorClass: Option[Class[_]]): AvroPhaseDefinition = {
    phaseClass
        .map { pclass =>
          val phaseClassName = pclass.getName
          val extractorClassName = extractorClass
              .map { _.getName }
              .getOrElse(null)

          AvroPhaseDefinition
              .newBuilder()
              .setExtractorClass(extractorClassName)
              .setPhaseClass(phaseClassName)
              .build()
        }
        .getOrElse(null)
  }

  /**
   * Builds a map of input specifications from its avro record representation.
   *
   * @param inputSpecs a map of avro input specifications to build from.
   * @return a map of [[org.kiji.modeling.config.InputSpec]].
   */
  def inputSpecsFromAvro(inputSpecs: java.util.Map[String, AvroInputSpec]):
      Map[String, InputSpec] = {
    inputSpecs.asScala.mapValues(inputSpecFromAvro).map(kv => (kv._1,kv._2)).toMap
  }

  /**
   * Builds an input specification from its avro record representation.
   *
   * @param inputSpec to build from.
   * @return a populated input specification.
   */
  def inputSpecFromAvro(inputSpec: AvroInputSpec): InputSpec = {
    // Get provided specifications (only one should be not null).
    val kijiSpecification: Option[InputSpec] = Option(inputSpec.getKijiSpecification)
        .map { avroSpec: AvroKijiInputSpec => kijiInputSpecFromAvro(avroSpec) }

    val textSpecification: Option[InputSpec] = Option(inputSpec.getTextSpecification)
        .map { avroSpec: AvroTextSourceSpec =>
          TextSourceSpec(path = avroSpec.getFilePath)
        }

    val seqFileSpecification: Option[InputSpec] = Option(inputSpec.getSequenceFileSpecification)
        .map { avroSpec: AvroSequenceFileSourceSpec =>
          SequenceFileSourceSpec(
              path = avroSpec.getFilePath,
              keyField = Option(avroSpec.getKeyField),
              valueField = Option(avroSpec.getValueField))
        }

    // Ensure that only one specification is available.
    val specifications: Seq[InputSpec] = kijiSpecification.toSeq ++
        textSpecification ++
        seqFileSpecification
    if (specifications.length > 1) {
      throw new ValidationException("Multiple InputSpec types provided: %s".format(specifications))
    } else if (specifications.length == 0) {
      throw new ValidationException("No InputSpec provided.")
    }

    // Return the one valid specification.
    specifications.head
  }

  /**
   * Converts a map of [[org.kiji.modeling.config.InputSpec]] to its avro representation.
   *
   * @param inputSpecs to convert.
   * @return a Java map of avro records.
   */
  def inputSpecsToAvro(inputSpecs: Map[String, InputSpec]):
      java.util.Map[String, AvroInputSpec] = {
    inputSpecs.mapValues(inputSpecToAvro).asJava
  }

  /**
   * Converts an input specification to its avro record representation.
   *
   * @param inputSpec to convert.
   * @return an avro record.
   */
  def inputSpecToAvro(inputSpec: InputSpec): AvroInputSpec = {
    inputSpec match {
      case x: KijiInputSpec => {
        val spec = kijiInputSpecToAvro(x)
        AvroInputSpec
            .newBuilder()
            .setKijiSpecification(spec)
            .build()
      }
      case TextSourceSpec(path) => {
        val spec = AvroTextSourceSpec
            .newBuilder()
            .setFilePath(path)
            .build()

        AvroInputSpec
            .newBuilder()
            .setTextSpecification(spec)
            .build()
      }
      case SequenceFileSourceSpec(path, keyFieldOption, valueFieldOption) => {
        val spec = AvroSequenceFileSourceSpec
            .newBuilder()
            .setFilePath(path)
            .setKeyField(keyFieldOption.getOrElse(null))
            .setValueField(valueFieldOption.getOrElse(null))
            .build()

        AvroInputSpec
            .newBuilder()
            .setSequenceFileSpecification(spec)
            .build()
      }
    }
  }

  /**
   * Builds a map of output specifications from its avro record representation.
   *
   * @param outputSpecs a map of avro output specifications to build from.
   * @return a map of [[org.kiji.modeling.config.OutputSpec]].
   */
  def outputSpecsFromAvro(outputSpecs: java.util.Map[String, AvroOutputSpec]):
      Map[String, OutputSpec] = {
    outputSpecs.asScala.mapValues(outputSpecFromAvro).map(kv => (kv._1,kv._2)).toMap
  }

  /**
   * Builds an output specification from its avro record representation.
   *
   * @param outputSpec to build from.
   * @return a populated output specification.
   */
  def outputSpecFromAvro(outputSpec: AvroOutputSpec): OutputSpec = {
    // Get provided specifications (only one should be not null).
    val kijiSpecification: Option[OutputSpec] = Option(outputSpec.getKijiSpecification)
        .map { avroSpec: AvroKijiOutputSpec => kijiOutputSpecFromAvro(avroSpec) }

    val kijiColumnSpecification: Option[OutputSpec] = Option(outputSpec.getKijiColumnSpecification)
        .map { avroSpec: AvroKijiSingleColumnOutputSpec =>
          kijiSingleColumnOutputSpecFromAvro(avroSpec) }

    val textSpecification: Option[OutputSpec] = Option(outputSpec.getTextSpecification)
        .map { avroSpec: AvroTextSourceSpec => TextSourceSpec(path = avroSpec.getFilePath) }

    val seqFileSpecification: Option[OutputSpec] = Option(outputSpec.getSequenceFileSpecification)
        .map { avroSpec: AvroSequenceFileSourceSpec =>
          SequenceFileSourceSpec(
              path = avroSpec.getFilePath,
              keyField = Option(avroSpec.getKeyField),
              valueField = Option(avroSpec.getValueField))
        }

    // Ensure that only one specification is available.
    val specifications: Seq[OutputSpec] = kijiSpecification.toSeq ++
        kijiColumnSpecification ++
        textSpecification ++
        seqFileSpecification
    if (specifications.length > 1) {
      throw new ValidationException("Multiple OutputSpec types provided: %s".format(specifications))
    } else if (specifications.length == 0) {
      throw new ValidationException("No OutputSpec provided.")
    }

    // Return the one valid specification.
    specifications.head
  }

  /**
   * Converts a map of [[org.kiji.modeling.config.OutputSpec]] to its avro representation.
   *
   * @param outputSpecs to convert.
   * @return a Java map of avro records.
   */
  def outputSpecsToAvro(outputSpecs: Map[String, OutputSpec]):
      java.util.Map[String, AvroOutputSpec] = {
    outputSpecs.mapValues(outputSpecToAvro).asJava
  }

  /**
   * Converts an output specification to its avro record representation.
   *
   * @param outputSpec to convert.
   * @return an avro record.
   */
  def outputSpecToAvro(outputSpec: OutputSpec): AvroOutputSpec = {
    outputSpec match {
      case spec: KijiOutputSpec => {
        kijiOutputSpecToAvro(spec)
      }
      case KijiSingleColumnOutputSpec(uri, outputColumn) => {
        val spec = AvroKijiSingleColumnOutputSpec
            .newBuilder()
            .setTableUri(uri)
            .setOutputColumn(columnOutputSpecToAvro(outputColumn))
            .build()

        AvroOutputSpec
            .newBuilder()
            .setKijiColumnSpecification(spec)
            .build()
      }
      case TextSourceSpec(path) => {
        val spec = AvroTextSourceSpec
            .newBuilder()
            .setFilePath(path)
            .build()

        AvroOutputSpec
            .newBuilder()
            .setTextSpecification(spec)
            .build()
      }
      case SequenceFileSourceSpec(path, keyFieldOption, valueFieldOption) => {
        val spec = AvroSequenceFileSourceSpec
            .newBuilder()
            .setFilePath(path)
            .setKeyField(keyFieldOption.getOrElse(null))
            .setValueField(valueFieldOption.getOrElse(null))
            .build()

        AvroOutputSpec
            .newBuilder()
            .setSequenceFileSpecification(spec)
            .build()
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Begin code for generating KijiInputSpec and KijiOutputSpec from avro

  /**
    * Builds a filter specification from its avro record representation.
    *
    * @param filter to build from.
    * @return a populated filter specification.
    */
  def filterFromAvro(filter: AvroFilter): ColumnFilterSpec = {
    // Get provided filter specifications (only one should be not null).
    val andFilter: Option[ColumnFilterSpec] = Option(filter.getAndFilter)
        .map { components => AndFilterSpec(components.asScala.map { filterFromAvro }) }
    val orFilter: Option[ColumnFilterSpec] = Option(filter.getOrFilter)
        .map { components => OrFilterSpec(components.asScala.map { filterFromAvro }) }
    val rangeFilter: Option[ColumnFilterSpec] = Option(filter.getRangeFilter)
        .map { rangeFilter =>
          new ColumnRangeFilterSpec(
              minimum = Option(rangeFilter.getMinQualifier),
              maximum = Option(rangeFilter.getMaxQualifier),
              minimumIncluded = rangeFilter.getMinIncluded,
              maximumIncluded = rangeFilter.getMaxIncluded)
        }
    val regexFilter: Option[ColumnFilterSpec] = Option(filter.getRegexFilter)
        .map { regexFilter => new RegexQualifierFilterSpec(regexFilter.getRegex) }

    // Ensure that only one filter specification is available.
    val filters: Seq[ColumnFilterSpec] =
        andFilter.toSeq ++ orFilter ++ rangeFilter ++ regexFilter
    if (filters.length > 1) {
      throw new ValidationException("Multiple filter types provided: %s".format(filters))
    } else if (filters.length == 0) {
      throw new ValidationException("No filter provided.")
    }

    // Return the one valid filter specification.
    filters.head
  }

  /**
   * Builds a SchemaSpec from its avro representation.
   *
   * @param avroSchema Avro representation of the schema.
   * @return The schema as a proper SchemaSpec.
   */
  def schemaSpecFromAvro(avroSchema: AvroSchemaSpec): SchemaSpec = {
    // TODO: Should check that only one of these is set valid
    if (null == avroSchema) {
      SchemaSpec.Writer
    } else if (avroSchema.getDefaultReader) {
      SchemaSpec.DefaultReader
    } else if (avroSchema.getGeneric != null) {
      SchemaSpec.Generic((new Schema.Parser()).parse(avroSchema.getGeneric))
    } else if (avroSchema.getSpecific != null) {
      // Get the name of the SpecificRecord
      val avroSpecificSchema = avroSchema.getSpecific
      try {
        // Try to convert the specific record string into an actual class
        val currentSchemaClass = Class
            .forName(avroSpecificSchema.getClassName)
            .asInstanceOf[Class[_ <: SpecificRecord]]

        // The class name could have stayed the same while the schema changed, so check that the
        // serialized schema matches the schema of the version of this record that is is present
        // now.
        val currentSchemaString: String = currentSchemaClass.newInstance.getSchema.toString
        val serializedSchemaString: String = avroSpecificSchema.getSchemaString

        if (currentSchemaString != serializedSchemaString) {
          val msg: String = "Previously serialized schema for SpecificRecord class " +
              avroSpecificSchema.getClassName + " is different from schema in current scope " +
              "(" + serializedSchemaString + " versus " + currentSchemaString + ")"
            throw new ValidationException(msg)
        }
        SchemaSpec.Specific(currentSchemaClass)
      } catch {
        case e: java.lang.ClassNotFoundException => {
          val msg: String = "Could not create Avro SpecificRecord class for " + avroSpecificSchema +
              ". (" + e + ")"
          throw new ValidationException(msg)
        }
      }
    } else {
      throw new ValidationException("Could not create SchemaSpec from Avro.")
    }
  }

  /**
   * Builds an input-column specification from its avro record representation.
   *
   * Note that the avroColumn is of type `Any` because it comes from an avro union.
   *
   * @param avroColumn Avro description of the input-column specification.
   * @return A `ColumnInputSpec` created from the Avro description.
   */
  def columnInputSpecFromAvro(avroColumn: Any): ColumnInputSpec = avroColumn match {
    case col: AvroQualifiedColumnInputSpec => {
      val filter: Option[ColumnFilterSpec] = Option(col.getFilter).map(filterFromAvro)
      val paging: PagingSpec = if (0 == col.getPageSize) {
        PagingSpec.Off
      } else {
        PagingSpec.Cells(col.getPageSize)
      }
      val schema: SchemaSpec = schemaSpecFromAvro(col.getSchemaSpec)

      QualifiedColumnInputSpec(
          family = col.getFamily,
          qualifier = col.getQualifier,
          maxVersions = col.getMaxVersions,
          filter = filter,
          paging = paging,
          schemaSpec = schema)
    }
    case col: AvroColumnFamilyInputSpec => {
      val filter: Option[ColumnFilterSpec] = Option(col.getFilter).map(filterFromAvro)
      val paging: PagingSpec = if (0 == col.getPageSize) {
        PagingSpec.Off
      } else {
        PagingSpec.Cells(col.getPageSize)
      }
      val schema: SchemaSpec = schemaSpecFromAvro(col.getSchemaSpec)

      ColumnFamilyInputSpec(
          family = col.getFamily,
          maxVersions = col.getMaxVersions,
          filter = filter,
          paging = paging,
          schemaSpec = schema)
    }
    // TODO: Real error message
    case col: Any => throw new ValidationException(
        "Cannot create ColumnInputSpec from Avro " + col.getClass)
  }

  /**
   * Builds an output-column specification from an Avro description.
   *
   * Note that the avroColumn is of type `Any` because it comes from an avro union.
   *
   * @param avroColumn Avro description of the output-column specification.
   * @return A `ColumnOutputSpec` created from the Avro description.
   */
  def columnOutputSpecFromAvro(avroColumn: Any): ColumnOutputSpec = avroColumn match {
    case col: AvroQualifiedColumnOutputSpec => {
      val schema: SchemaSpec = schemaSpecFromAvro(col.getSchemaSpec)
      QualifiedColumnOutputSpec(
          family = col.getFamily,
          qualifier = col.getQualifier,
          schemaSpec = schema)
    }
    case col: AvroColumnFamilyOutputSpec => {
      val schema: SchemaSpec = schemaSpecFromAvro(col.getSchemaSpec)
      ColumnFamilyOutputSpec(
          family = col.getFamily,
          qualifierSelector = Symbol(col.getQualifierSelector),
          schemaSpec = schema)
    }
    // TODO: Real error message
    case col: Any => throw new ValidationException("Cannot create ColumnOutputSpec from Avro " +
      col.getClass + " " + col)
  }

  /**
   * Builds a Kiji input specification from an Avro description.
   *
   * @param avroSpec Avro description of the Kiji input specification.
   * @return A `KijiInputSpec` created from the Avro description.
   */
  def kijiInputSpecFromAvro(avroSpec: AvroKijiInputSpec): KijiInputSpec = {
    val tableUri: String = avroSpec.getTableUri

    val timeRange: TimeRange = Between(
        avroSpec.getTimeRange.min_timestamp,
        avroSpec.getTimeRange.max_timestamp)

    val columnsToFields: Map[_ <: ColumnInputSpec, Symbol] = avroSpec
        // Gives us pairs of (Avro column spec, field name)
        .getColumnsToFields
        .asScala

        // Turn field name into a symbol and get a column input spec object
        .map { columnAndField => (
            columnInputSpecFromAvro(columnAndField.getColumn),
            Symbol(columnAndField.getTupleFieldName))
        }
        .toMap

    KijiInputSpec(
        tableUri = tableUri,
        timeRange = timeRange,
        columnsToFields = columnsToFields)
  }

  /**
   * Builds a Kiji output specification from an Avro description.
   *
   * @param avroSpec Avro description of the Kiji output specification.
   * @return A `KijiOutputSpec` created from the Avro description.
   */
  def kijiOutputSpecFromAvro(avroSpec: AvroKijiOutputSpec): KijiOutputSpec = {
    val tableUri: String = avroSpec.getTableUri

    // Field to use as timestamp
    val timestampField = avroSpec.getTimestampField match {
      case null => None
      case timestampField: String => Some(Symbol(timestampField))
    }

    val fieldsToColumns: Map[Symbol, _ <: ColumnOutputSpec] = avroSpec
        // Gives us pairs of (field name, Avro column spec)
        .getFieldsToColumns
        .asScala

        // Turn field name into a symbol and get a column output spec object
        .map { columnAndField => (
            Symbol(columnAndField.getTupleFieldName),
            columnOutputSpecFromAvro(columnAndField.getColumn))
        }
        .toMap

    KijiOutputSpec(
        tableUri = tableUri,
        fieldsToColumns = fieldsToColumns,
        timestampField = timestampField)
  }

  /**
   * Builds a Kiji single-column output specification from an Avro description.
   *
   * @param avroSpec Avro description of the Kiji single-column output specification.
   * @return A `KijiSingleColumnOutputSpec` created from the Avro description.
   */
  def kijiSingleColumnOutputSpecFromAvro(
      avroSpec: AvroKijiSingleColumnOutputSpec): KijiSingleColumnOutputSpec = {
    val tableUri: String = avroSpec.getTableUri
    val outputColumn: ColumnOutputSpec = columnOutputSpecFromAvro(avroSpec.getOutputColumn)

    KijiSingleColumnOutputSpec(tableUri = tableUri, outputColumn = outputColumn)
  }

  // -----------------------------------------------------------------------------------------------
  // Begin code for generating avro from KijiInputSpec and KijiOutputSpec

  /**
   * Converts a KijiExpress column filter specification into an Avro description.
   *
   * @param filter The filter to convert into an Avro description.
   * @return The Avro description of the filter.
   */
  def filterToAvro(filter: ColumnFilterSpec): AvroFilter = {
    filter match {
      case AndFilterSpec(filters) => {
        val avroFilters: java.util.List[AvroFilter] = filters
            .map { filterToAvro }
            .asJava

        AvroFilter
            .newBuilder()
            .setAndFilter(avroFilters)
            .build()
      }
      case OrFilterSpec(filters) => {
        val avroFilters: java.util.List[AvroFilter] = filters
            .map { filterToAvro }
            .asJava

        AvroFilter
            .newBuilder()
            .setOrFilter(avroFilters)
            .build()
      }
      case ColumnRangeFilterSpec(minimum, maximum, minimumIncluded, maximumIncluded) => {
        val rangeFilter = AvroColumnRangeFilter
            .newBuilder()
            .setMinQualifier(minimum.getOrElse(null))
            .setMaxQualifier(maximum.getOrElse(null))
            .setMinIncluded(minimumIncluded)
            .setMaxIncluded(maximumIncluded)
            .build()

        AvroFilter
            .newBuilder()
            .setRangeFilter(rangeFilter)
            .build()
      }
      case RegexQualifierFilterSpec(regex) => {
        val regexFilter = AvroRegexQualifierFilter
            .newBuilder()
            .setRegex(regex)
            .build()

        AvroFilter
            .newBuilder()
            .setRegexFilter(regexFilter)
            .build()
      }
    }
  }

  /**
   * Converts a column schema specification into an Avro description.
   *
   * @param schemaSpec The column schema specification to convert into an Avro description.
   * @return The Avro description of the schema.
   */
  def schemaSpecToAvro(schemaSpec: SchemaSpec): AvroSchemaSpec = {
    schemaSpec match {
      case SchemaSpec.Writer => null
      case SchemaSpec.DefaultReader => AvroSchemaSpec
          .newBuilder()
          .setDefaultReader(true)
          .build()
      case SchemaSpec.Generic(genericSchema) => AvroSchemaSpec
          .newBuilder()
          .setGeneric(genericSchema.toString)
          .build()
      case SchemaSpec.Specific(klass) => {
        // Create an instance of klass to get the schema
        val recordInst: SpecificRecord = klass.newInstance

        val avroSpecificSchema = AvroSpecificSchemaSpec
            .newBuilder()
            .setClassName(klass.getName)
            .setSchemaString(recordInst.getSchema.toString)
            .build()
        AvroSchemaSpec
            .newBuilder()
            .setSpecific(avroSpecificSchema)
            .build()
      }
      case _ =>
          throw new ValidationException(
              "Not sure how to encode schema " + schemaSpec + " into Avro")
    }
  }

  /**
   * Converts an input column specification into an Avro description.
   *
   * @param column The column specification to convert into an Avro description.
   * @return The Avro description of the column.
   */
  def columnInputSpecToAvro(column: ColumnInputSpec): Any = column match {
    case col: QualifiedColumnInputSpec => AvroQualifiedColumnInputSpec
        .newBuilder()
        .setFamily(col.family)
        .setQualifier(col.qualifier)
        .setMaxVersions(col.maxVersions)
        .setFilter(col.filter.map(filterToAvro).getOrElse(null))
        .setPageSize(col.paging.cellsPerPage.getOrElse(0))
        .setSchemaSpec(schemaSpecToAvro(col.schemaSpec))
        .build()
    case col: ColumnFamilyInputSpec => AvroColumnFamilyInputSpec
        .newBuilder()
        .setFamily(col.family)
        .setMaxVersions(col.maxVersions)
        .setFilter(col.filter.map(filterToAvro).getOrElse(null))
        .setPageSize(col.paging.cellsPerPage.getOrElse(0))
        .setSchemaSpec(schemaSpecToAvro(col.schemaSpec))
        .build()
    case col: Any => throw new ValidationException(
        "Cannot convert ColumnInputSpec to Avro " + col.getClass)
  }

  /**
   * Converts an output column specification into an Avro description.
   *
   * @param column The column specification to convert into an Avro description.
   * @return The Avro description of the column.
   */
  def columnOutputSpecToAvro(column: ColumnOutputSpec): Any = column match {
    case col: QualifiedColumnOutputSpec => AvroQualifiedColumnOutputSpec
        .newBuilder()
        .setFamily(col.family)
        .setQualifier(col.qualifier)
        .setSchemaSpec(schemaSpecToAvro(col.schemaSpec))
        .build()
    case col: ColumnFamilyOutputSpec => AvroColumnFamilyOutputSpec
        .newBuilder()
        .setFamily(col.family)
        .setQualifierSelector(col.qualifierSelector.name)
        .setSchemaSpec(schemaSpecToAvro(col.schemaSpec))
        .build()
    case col: Any => throw new ValidationException(
        "Cannot convert ColumnOutputSpec to Avro " + col.getClass)
  }

  /**
   * Converts a time range specification into an Avro description.
   *
   * @param timeRange The time range specification to convert into an Avro description.
   * @return The Avro description of the time range.
   */
  def timeRangeToAvro(timeRange: TimeRange): AvroTimeRange = {
    AvroTimeRange
        .newBuilder()
        .setMinTimestamp(timeRange.begin)
        .setMaxTimestamp(timeRange.end)
        .build()
  }

  /**
   * Converts a Kiji input specification into an Avro description.
   *
   * @param inputSpec The Kiji input specification to convert into an Avro description.
   * @return The Avro description of the Kiji input specification.
   */
  def kijiInputSpecToAvro(inputSpec: KijiInputSpec): AvroKijiInputSpec = {
    val avroColumns = inputSpec
        .columnsToFields
        .toList
        .map( columnAndField => {
          val (col: ColumnInputSpec, field: Symbol) = columnAndField
          AvroInputFieldBinding
              .newBuilder()
              .setColumn(columnInputSpecToAvro(col))
              .setTupleFieldName(field.name)
              .build()
        })
        .asJava

    AvroKijiInputSpec
        .newBuilder()
        .setTableUri(inputSpec.tableUri)
        .setTimeRange(timeRangeToAvro(inputSpec.timeRange))
        .setColumnsToFields(avroColumns)
        .build()
  }

  /**
   * Converts a Kiji output specification into an Avro description.
   *
   * @param outputSpec The Kiji output specification to convert into an Avro description.
   * @return The Avro description of the Kiji output specification.
   */
  def kijiOutputSpecToAvro(outputSpec: KijiOutputSpec): AvroOutputSpec = {
    val timestampField: Option[String] = outputSpec.timestampField.map(_.name)

    val avroColumns = outputSpec
        .fieldsToColumns
        .toList
        .map( fieldAndColumn => {
          val (field: Symbol, col: ColumnOutputSpec) = fieldAndColumn
          AvroOutputFieldBinding
              .newBuilder()
              .setColumn(columnOutputSpecToAvro(col))
              .setTupleFieldName(field.name)
              .build()
        })
        .asJava

    val spec = AvroKijiOutputSpec
        .newBuilder()
        .setTableUri(outputSpec.tableUri)
        .setTimestampField(timestampField.getOrElse(null))
        .setFieldsToColumns(avroColumns)
        .build()

    AvroOutputSpec
        .newBuilder()
        .setKijiSpecification(spec)
        .build()
  }

  /**
   * Converts a Kiji single-column output specification into an Avro description.
   *
   * @param outputSpec The Kiji single-column output spec to convert into an Avro description.
   * @return The Avro description of the Kiji output specification.
   */
  def kijiSingleColumnOutputSpecToAvro(
      outputSpec: KijiSingleColumnOutputSpec): AvroKijiSingleColumnOutputSpec = {
    AvroKijiSingleColumnOutputSpec
        .newBuilder()
        .setTableUri(outputSpec.tableUri)
        .setOutputColumn(columnOutputSpecToAvro(outputSpec.outputColumn))
        .build()
  }
}
