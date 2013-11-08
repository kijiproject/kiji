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

import org.kiji.express.flow.AndFilter
import org.kiji.express.flow.Between
import org.kiji.express.flow.ColumnRangeFilter
import org.kiji.express.flow.ColumnRequestInput
import org.kiji.express.flow.ColumnRequestOutput
import org.kiji.express.flow.ExpressColumnFilter
import org.kiji.express.flow.OrFilter
import org.kiji.express.flow.QualifiedColumnRequestOutput
import org.kiji.express.flow.RegexQualifierFilter
import org.kiji.modeling.Evaluator
import org.kiji.modeling.Extractor
import org.kiji.modeling.Preparer
import org.kiji.modeling.Scorer
import org.kiji.modeling.Trainer
import org.kiji.modeling.avro.AvroColumn
import org.kiji.modeling.avro.AvroColumnRangeFilter
import org.kiji.modeling.avro.AvroDataRequest
import org.kiji.modeling.avro.AvroEvaluateEnvironment
import org.kiji.modeling.avro.AvroFieldBinding
import org.kiji.modeling.avro.AvroFilter
import org.kiji.modeling.avro.AvroInputSpec
import org.kiji.modeling.avro.AvroKeyValueStoreSpec
import org.kiji.modeling.avro.AvroKeyValueStoreType
import org.kiji.modeling.avro.AvroKijiInputSpec
import org.kiji.modeling.avro.AvroKijiOutputSpec
import org.kiji.modeling.avro.AvroKijiSingleColumnOutputSpec
import org.kiji.modeling.avro.AvroModelDefinition
import org.kiji.modeling.avro.AvroModelEnvironment
import org.kiji.modeling.avro.AvroOutputSpec
import org.kiji.modeling.avro.AvroPhaseDefinition
import org.kiji.modeling.avro.AvroPrepareEnvironment
import org.kiji.modeling.avro.AvroProperty
import org.kiji.modeling.avro.AvroRegexQualifierFilter
import org.kiji.modeling.avro.AvroScoreEnvironment
import org.kiji.modeling.avro.AvroSequenceFileSourceSpec
import org.kiji.modeling.avro.AvroTextSourceSpec
import org.kiji.modeling.avro.AvroTrainEnvironment
import org.kiji.modeling.config.EvaluateEnvironment
import org.kiji.modeling.config.FieldBinding
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
    // scalastyle:off null
    val avroPreparerClass = modelDefinition
        .preparerClass
        .map { _.getName }
        .getOrElse(null)

    // Build the Train phase's definition.
    val avroTrainerClass = modelDefinition
        .trainerClass
        .map { _.getName }
        .getOrElse(null)
    // scalastyle:on null

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

    // scalastyle:off null
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
    // scalastyle:on null
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
    val outputSpec: KijiSingleColumnOutputSpec = {
      val avroOutputSpec = environment.getOutputSpec
      KijiSingleColumnOutputSpec(
          tableUri = avroOutputSpec.getTableUri,
          outputColumn = QualifiedColumnRequestOutput(avroOutputSpec.getOutputColumn))
    }
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
    val avroOutputSpec: AvroKijiSingleColumnOutputSpec = {
      val outputSpec = environment.outputSpec
      AvroKijiSingleColumnOutputSpec
          .newBuilder()
          .setTableUri(outputSpec.tableUri)
          .setOutputColumn(outputSpec.outputColumn.columnName.toString)
          .build()
    }
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
          KijiSingleColumnOutputSpec(
              tableUri = avroSpec.getTableUri,
              outputColumn = QualifiedColumnRequestOutput(avroSpec.getOutputColumn))
        }
    val textSpecification: Option[OutputSpec] = Option(outputSpec.getTextSpecification)
        .map { avroSpec: AvroTextSourceSpec =>
          TextSourceSpec(path = avroSpec.getFilePath)
        }
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
      throw new ValidationException("Multiple InputSpec types provided: %s".format(specifications))
    } else if (specifications.length == 0) {
      throw new ValidationException("No InputSpec provided.")
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
            .setOutputColumn(outputColumn.columnName.toString)
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
   * Builds a field binding from its avro record representation.
   *
   * @param fieldBinding to build from.
   * @return a populated field binding.
   */
  def fieldBindingFromAvro(fieldBinding: AvroFieldBinding): FieldBinding = {
    FieldBinding(
        tupleFieldName = fieldBinding.getTupleFieldName,
        storeFieldName = fieldBinding.getStoreFieldName)
  }

  /**
   * Converts a field binding to its avro record representation.
   *
   * @param fieldBindings to convert.
   * @return an avro record.
   */
  def fieldBindingToAvro(fieldBindings: FieldBinding): AvroFieldBinding = {
    AvroFieldBinding
        .newBuilder()
        .setTupleFieldName(fieldBindings.tupleFieldName)
        .setStoreFieldName(fieldBindings.storeFieldName)
        .build()
  }

  /**
    * Builds a filter specification from its avro record representation.
    *
    * @param filter to build from.
    * @return a populated filter specification.
    */
  def filterFromAvro(filter: AvroFilter): ExpressColumnFilter = {
    // Get provided filter specifications (only one should be not null).
    val andFilter: Option[ExpressColumnFilter] = Option(filter.getAndFilter)
        .map { components => AndFilter(components.asScala.map { filterFromAvro }) }
    val orFilter: Option[ExpressColumnFilter] = Option(filter.getOrFilter)
        .map { components => OrFilter(components.asScala.map { filterFromAvro }) }
    val rangeFilter: Option[ExpressColumnFilter] = Option(filter.getRangeFilter)
        .map { rangeFilter =>
          new ColumnRangeFilter(
              minimum = Option(rangeFilter.getMinQualifier),
              maximum = Option(rangeFilter.getMaxQualifier),
              minimumIncluded = rangeFilter.getMinIncluded,
              maximumIncluded = rangeFilter.getMaxIncluded)
        }
    val regexFilter: Option[ExpressColumnFilter] = Option(filter.getRegexFilter)
        .map { regexFilter => new RegexQualifierFilter(regexFilter.getRegex) }

    // Ensure that only one filter specification is available.
    val filters: Seq[ExpressColumnFilter] =
        andFilter.toSeq ++ orFilter ++ rangeFilter ++ regexFilter
    if (filters.length > 1) {
      throw new ValidationException("Multiple InputSpec types provided: %s".format(filters))
    } else if (filters.length == 0) {
      throw new ValidationException("No InputSpec provided.")
    }

    // Return the one valid filter specification.
    filters.head
  }

  /**
   * Converts a filter specification to its avro record representation.
   *
   * @param filter to convert.
   * @return an avro record.
   */
  def filterToAvro(filter: ExpressColumnFilter): AvroFilter = {
    filter match {
      case AndFilter(filters) => {
        val avroFilters: java.util.List[AvroFilter] = filters
            .map { filterToAvro }
            .asJava

        AvroFilter
            .newBuilder()
            .setAndFilter(avroFilters)
            .build()
      }
      case OrFilter(filters) => {
        val avroFilters: java.util.List[AvroFilter] = filters
            .map { filterToAvro }
            .asJava

        AvroFilter
            .newBuilder()
            .setOrFilter(avroFilters)
            .build()
      }
      case ColumnRangeFilter(minimum, maximum, minimumIncluded, maximumIncluded) => {
        // scalastyle:off null
        val rangeFilter = AvroColumnRangeFilter
            .newBuilder()
            .setMinQualifier(minimum.getOrElse(null))
            .setMaxQualifier(maximum.getOrElse(null))
            .setMinIncluded(minimumIncluded)
            .setMaxIncluded(maximumIncluded)
            .build()
        // scalastyle:on null

        AvroFilter
            .newBuilder()
            .setRangeFilter(rangeFilter)
            .build()
      }
      case RegexQualifierFilter(regex) => {
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
   * Retrieves the class for the provided phase implementation class name handling errors
   * properly.
   *
   * @param phaseImplName to build phase class from.
   * @param phase that the resulting class should belong to.
   * @tparam T is the type of the phase class.
   * @return the phase implementation class.
   */
  private[modeling] def getClassForPhase[T](phaseImplName: String, phase: Class[T]): Class[T] = {
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
  private[modeling] def phaseDefinitionToAvro(
      phaseClass: Option[Class[_]],
      extractorClass: Option[Class[_]]): AvroPhaseDefinition = {
    // scalastyle:off null
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
    // scalastyle:on null
  }

  /**
   * Create a KijiInputSpec from an Avro description.
   *
   * @param avroInputSpec The avro description of the input spec.
   * @return The same input spec, but as a [[org.kiji.modeling.config.KijiInputSpec]]
   */
  def kijiInputSpecFromAvro(avroInputSpec: AvroKijiInputSpec): KijiInputSpec = {
    // URI
    val tableUri: String = avroInputSpec.getTableUri

    // TimeRange
    val avroDataRequest = avroInputSpec.getDataRequest
    val timeRange = Between(avroDataRequest.getMinTimestamp, avroDataRequest.getMaxTimestamp)

    // Field bindings
    val fieldBindings = avroInputSpec.getFieldBindings.asScala
    val columnNamesToFieldNames: Map[String, String] = fieldBindings
        .map { x => (x.getStoreFieldName, x.getTupleFieldName) }
        .toMap

    // Columns
    val columns: Map[_ <: ColumnRequestInput, Symbol] = avroDataRequest
        .getColumnDefinitions
        .asScala
        .map { avroColumn: AvroColumn =>
          val filter = Option(avroColumn.getFilter)
              .map { x => filterFromAvro(x) }

          val columnName = avroColumn.getName

          val column = ColumnRequestInput(
              column = columnName,
              maxVersions = avroColumn.getMaxVersions,
              filter = filter)

          // TODO (EXP-264): Errors of this type used to be caught by the validation code, after a
          // KijiInputSpec had been constructed from Avro.  Here we cannot even get to the
          // validation phase, because we no longer can construct a KijiInputSpec containing columns
          // without field bindings.  We need to determine whether just throwing a
          // ValidationException here is okay, or if we need to do something else.
          if (!columnNamesToFieldNames.contains(columnName)) {
            val msg = "Specified AvroKijiInputSpec with column " + columnName +
                ", but without a valid field binding."
            throw new ValidationException(msg)
          }

          val fieldName: String = columnNamesToFieldNames(columnName)

          (column, Symbol(fieldName))
        }
        .toMap

    KijiInputSpec(
        tableUri = tableUri,
        timeRange = timeRange,
        columnsToFields = columns)
  }

  /**
   * Create an Avro representation of a KijiInputSpec.
   *
   * @param inputSpec The [[org.kiji.modeling.config.KijiInputSpec]] to transform with Avro.
   * @return an Avro version of the input spec.
   */
  def kijiInputSpecToAvro(inputSpec: KijiInputSpec): AvroKijiInputSpec = {
    // Turn a flow ColumnRequestInput into a modeling ExpressColumnRequest
    def columnRequestInputToAvroColumn(column: ColumnRequestInput): AvroColumn = {
      AvroColumn
          .newBuilder()
          .setName(column.columnName.toString)
          .setMaxVersions(column.maxVersions)
          .setFilter(column.filter.map{filterToAvro}.getOrElse(null))
          .build()
    }

    // Create an AvroDataRequest from the KijiInputSpec
    def getAvroDataRequest: AvroDataRequest = {
      val columns: java.util.List[AvroColumn] = inputSpec
          .columnsToFields
          .keys
          .toList
          .map { columnRequestInputToAvroColumn }
          .asJava

      AvroDataRequest
          .newBuilder()
          .setMinTimestamp(inputSpec.timeRange.begin)
          .setMaxTimestamp(inputSpec.timeRange.end)
          .setColumnDefinitions(columns)
          .build()
    }

    // Create FieldBindings (that will become AvroFieldBindings) from the KijiInputSpec.
    def getBindings: Seq[FieldBinding] = {
      inputSpec
          .columnsToFields
          .toList
          // Now we have a list of (ColumnRequestInput, Symbol) tuples
          .map { case (column: ColumnRequestInput, field: Symbol) => {
            val tupleFieldName = field.name
            val storeFieldName = column.columnName.toString
            FieldBinding(tupleFieldName, storeFieldName)
          }}
    }

  AvroKijiInputSpec
      .newBuilder()
      .setTableUri(inputSpec.tableUri)
      .setDataRequest(getAvroDataRequest)
      .setFieldBindings(getBindings.map { fieldBindingToAvro } .asJava)
      .build()

  }

  /**
   * Create a KijiOutputSpec from an Avro description.
   *
   * @param avroOutputSpec The avro description of the output spec.
   * @return The same output spec, but as a [[org.kiji.modeling.config.KijiOutputSpec]]
   */
  def kijiOutputSpecFromAvro(avroOutputSpec: AvroKijiOutputSpec): KijiOutputSpec = {
    // URI
    val tableUri: String = avroOutputSpec.getTableUri

    // Field bindings
    val fieldBindings = avroOutputSpec.getFieldBindings.asScala

    // Field to use as timestamp
    val timestampField = avroOutputSpec.getTimestampField match {
      case null => None
      case timestampField: String => Some(Symbol(timestampField))
    }

    // Avro spec does not yet support any fancy stuff for output column
    val columns: Map[Symbol, _ <: ColumnRequestOutput] = fieldBindings
        .map { fieldBinding => {
          // Convert store field name into ColumnRequestOutput
          val column = QualifiedColumnRequestOutput(fieldBinding.getStoreFieldName)
          (Symbol(fieldBinding.getTupleFieldName), column)
        }}
        .toMap

    KijiOutputSpec(
        tableUri = tableUri,
        fieldsToColumns = columns,
        timestampField = timestampField)
  }

  /**
   * Create an Avro representation of a KijiOutputSpec.
   *
   * @param outputSpec The [[org.kiji.modeling.config.KijiOutputSpec]] to transform with Avro.
   * @return an Avro version of the output spec.
   */
  def kijiOutputSpecToAvro(outputSpec: KijiOutputSpec): AvroOutputSpec = {

    // Extract field bindings from column requests
    def getBindings: Seq[FieldBinding] = {
      outputSpec
          .fieldsToColumns
          .toList
          // Now we have a list of (ColumnRequestInput, Symbol) tuples
          .map { case (field: Symbol, column: ColumnRequestOutput) => {
            val tupleFieldName = field.name
            val storeFieldName = column.columnName.toString
            FieldBinding(tupleFieldName, storeFieldName)
          }}
    }

    val timestampField: Option[String] = outputSpec.timestampField.map(_.name)

    val spec = AvroKijiOutputSpec
        .newBuilder()
        .setTableUri(outputSpec.tableUri)
        .setFieldBindings(getBindings.map { fieldBindingToAvro } .asJava)
        .setTimestampField(timestampField.getOrElse(null))
        .build()

    AvroOutputSpec
        .newBuilder()
        .setKijiSpecification(spec)
        .build()
  }
}
