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

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source
import scala.Some

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.avro.AvroDataRequest
import org.kiji.express.avro.AvroInputSpec
import org.kiji.express.avro.AvroKijiInputSpec
import org.kiji.express.avro.AvroKijiOutputSpec
import org.kiji.express.avro.AvroKijiSingleColumnOutputSpec
import org.kiji.express.avro.AvroModelEnvironment
import org.kiji.express.avro.AvroOutputSpec
import org.kiji.express.avro.AvroPrepareEnvironment
import org.kiji.express.avro.AvroScoreEnvironment
import org.kiji.express.avro.AvroTrainEnvironment
import org.kiji.express.avro.ColumnSpec
import org.kiji.express.avro.KvStoreType
import org.kiji.express.util.Resources.doAndClose
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.KijiNameValidator
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson

/**
 * A ModelEnvironment is a specification describing how to execute a linked model definition.
 * This includes specifying:
 * <ul>
 *   <li>A model definition to run</li>
 *   <li>Mappings from logical names of data sources to their physical realizations</li>
 * </ul>
 *
 * A ModelEnvironment can be created programmatically:
 * {{{
 *   val dataRequest: ExpressDataRequest = new ExpressDataRequest(0, 38475687,
 *     new ExpressColumnRequest("info:in", 3, None) :: Nil)
 *
 *   val inputSpec = KijiInputSpec("kiji://myuri",
 *       dataRequest,
 *       Seq(FieldBinding("tuplename", "info:storefieldname")))
 *
 *   val outputSpec = KijiSingleColumnOutputSpec("kiji://myuri", "outputFamily:qualifier")
 *   val scoreEnv = Some(ScoreEnvironment(
 *       inputSpec,
 *       outputSpec,
 *       Seq(KVStore("KIJI_TABLE", "myname", Map("uri" -> "kiji://.env/default/table",
 *           "column" -> "info:email")),
 *           KVStore("AVRO_KV", "storename", Map("path" -> "/some/great/path")))))
 *
 *   val modelEnv = ModelEnvironment(
 *     "myname",
 *     "1.0.0",
 *     None,
 *     None,
 *     scoreEnv)
 * }}}
 *
 * Alternatively a ModelEnvironment can be created from JSON. JSON run specifications should
 * be written using the following format:
 * {{{
 * {
 *  "protocol_version":"model_environment-0.2.0",
 *  "name":"myRunProfile",
 *  "version":"1.0.0",
 *  "prepare_environment":{
 *    "org.kiji.express.avro.AvroPrepareEnvironment":{
 *      "input_config":{
 *        "specification":{
 *          "org.kiji.express.avro.AvroKijiInputSpec":{
 *            "table_uri":"kiji://.env/default/table",
 *            "data_request":{
 *              "min_timestamp":0,
 *              "max_timestamp":38475687,
 *              "column_definitions":[{
 *                "name":"info:in",
 *                "max_versions":3,
 *                "filter":{
 *                  "org.kiji.express.avro.RegexQualifierFilterSpec":{
 *                    "regex":"foo"
 *                  }
 *                }
 *              }]
 *            },
 *            "field_bindings":[{
 *              "tuple_field_name":"tuplename",
 *              "store_field_name":"info:storefieldname"
 *            }]
 *          }
 *        }
 *      },
 *      "output_config":{
 *        "specification":{
 *          "org.kiji.express.avro.AvroKijiOutputSpec":{
 *            "table_uri":"kiji://myuri",
 *            "field_bindings":[{
 *              "tuple_field_name":"outputtuple",
 *              "store_field_name":"info:out"
 *            }]
 *          }
 *        }
 *      },
 *      "kv_stores":[{
 *        "store_type":"AVRO_KV",
 *        "name":"side_data",
 *        "properties":[{
 *          "name":"path",
 *          "value":"/usr/src/and/so/on"
 *        }]
 *      }]
 *    }
 *  }
 * }
 * }}}
 *
 * To load a JSON model environment:
 * {{{
 * // Load a JSON string directly:
 * val myModelEnvironment: ModelEnvironment =
 *     ModelEnvironment.loadJson("""{ "name": "myIdentifier", ... }""")
 *
 * // Load a JSON file:
 * val myModelEnvironment2: ModelEnvironment =
 *     ModelEnvironment.loadJsonFile("/path/to/json/config.json")
 * }}}
 *
 * @param name of the model environment.
 * @param version of the model environment.
 * @param prepareEnvironment defining configuration details specific to the Prepare phase of a
 *     model. Optional.
 * @param trainEnvironment defining configuration details specific to the Train phase of a model.
 *     Optional.
 * @param scoreEnvironment defining configuration details specific to the Score phase of a model.
 *     Optional.
 * @param protocolVersion this model definition was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ModelEnvironment private[express] (
    val name: String,
    val version: String,
    val prepareEnvironment: Option[PrepareEnvironment],
    val trainEnvironment: Option[TrainEnvironment],
    val scoreEnvironment: Option[ScoreEnvironment],
    private[express] val protocolVersion: ProtocolVersion =
        ModelEnvironment.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model environment are valid.
  ModelEnvironment.validateModelEnvironment(this)

  /**
   * Serializes this model environment into a JSON string.
   *
   * @return a JSON string that represents this model environment.
   */
  def toJson(): String = {
    // Build an AvroPrepareEnvironment record.
    val avroPrepareEnvironment: Option[AvroPrepareEnvironment] = prepareEnvironment.map { env =>
      val inputConfig: AvroInputSpec = env
          .inputConfig
          .toAvroInputSpec
      val outputConfig: AvroOutputSpec = env
          .outputConfig
          .toAvroOutputSpec
      AvroPrepareEnvironment
          .newBuilder()
          .setInputConfig(inputConfig)
          .setOutputConfig(outputConfig)
          .setKvStores(env.kvstores.map { kvstore => kvstore.toAvroKVStore }.asJava)
          .build()
    }

    // Build an AvroTrainEnvironment record.
    val avroTrainEnvironment: Option[AvroTrainEnvironment] = trainEnvironment.map { env =>
      val inputConfig: AvroInputSpec = env
          .inputConfig
          .toAvroInputSpec
      val outputConfig: AvroOutputSpec = env
          .outputConfig
          .toAvroOutputSpec
      AvroTrainEnvironment
          .newBuilder()
          .setInputConfig(inputConfig)
          .setOutputConfig(outputConfig)
          .setKvStores(env.kvstores.map { kvstore => kvstore.toAvroKVStore }.asJava)
          .build()
    }

    // Build an AvroScoreEnvironment record.
    val avroScoreEnvironment: Option[AvroScoreEnvironment] = scoreEnvironment.map { env =>
      val inputConfig: AvroInputSpec = env
          .inputConfig
          .toAvroInputSpec
      val outputConfig: AvroOutputSpec = env
          .outputConfig
          .toAvroOutputSpec
      AvroScoreEnvironment
          .newBuilder()
          .setInputConfig(inputConfig)
          .setOutputConfig(outputConfig)
          .setKvStores(env.kvstores.map { kvstore => kvstore.toAvroKVStore }.asJava)
          .build()
    }

    // Build an AvroModelEnvironment record.
    val environment: AvroModelEnvironment = AvroModelEnvironment
        .newBuilder()
        .setName(name)
        .setVersion(version)
        .setProtocolVersion(protocolVersion.toString)
        .setPrepareEnvironment(avroPrepareEnvironment.getOrElse(null))
        .setTrainEnvironment(avroTrainEnvironment.getOrElse(null))
        .setScoreEnvironment(avroScoreEnvironment.getOrElse(null))
        .build()

    ToJson.toAvroJsonString(environment)
  }

  /**
   * Creates a new model environment with settings specified to this method. Any setting specified
   * to this method is used in the new model environment. Any unspecified setting will use the
   * value from this model environment in the new model environment.
   *
   * @param name of the model environment.
   * @param version of the model environment.
   * @param prepareEnvironment defining configuration details specific to the Prepare phase of a
   *     model.
   * @param trainEnvironment defining configuration details specific to the Train phase of a model.
   * @param scoreEnvironment defining configuration details specific to the Score phase of a model.
   */
  def withNewSettings(
      name: String = this.name,
      version: String = this.version,
      prepareEnvironment: Option[PrepareEnvironment] = this.prepareEnvironment,
      trainEnvironment: Option[TrainEnvironment] = this.trainEnvironment,
      scoreEnvironment: Option[ScoreEnvironment] = this.scoreEnvironment): ModelEnvironment = {
    new ModelEnvironment(
        name,
        version,
        prepareEnvironment,
        trainEnvironment,
        scoreEnvironment)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case environment: ModelEnvironment => {
        name == environment.name &&
            version == environment.version &&
            prepareEnvironment == environment.prepareEnvironment &&
            trainEnvironment == environment.trainEnvironment &&
            scoreEnvironment == environment.scoreEnvironment &&
            protocolVersion == environment.protocolVersion
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(
          name,
          version,
          prepareEnvironment,
          trainEnvironment,
          scoreEnvironment,
          protocolVersion)
}

/**
 * Companion object for ModelEnvironment. Contains constants related to model environments as well
 * as validation methods.
 */
object ModelEnvironment {
  /** Maximum model environment version we can recognize. */
  val MAX_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.2.0")

  /** Minimum model environment version we can recognize. */
  val MIN_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.2.0")

  /** Current ModelDefinition protocol version. */
  val CURRENT_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.2.0")

  /** Regular expression used to validate a model environment version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model definition. */
  private[express] val VALIDATION_MESSAGE = "One or more errors occurred while validating your" +
      " model environment. Please correct the problems in your model environment and try again."

  /**
   * Creates a new ModelEnvironment using the specified settings.
   * @param name of the model environment.
   * @param version of the model environment.
   * @param prepareEnvironment defining configuration details specific to the Prepare phase of
   *     a model.
   * @param scoreEnvironment defining configuration details specific to the Score phase of a
   *     model.
   * @return a model environment with the specified settings.
   */
  def apply(
      name: String,
      version: String,
      prepareEnvironment: Option[PrepareEnvironment],
      trainEnvironment: Option[TrainEnvironment],
      scoreEnvironment: Option[ScoreEnvironment]): ModelEnvironment = {
    new ModelEnvironment(
        name,
        version,
        prepareEnvironment,
        trainEnvironment,
        scoreEnvironment)
  }

  /**
   * Given a generic AvroInputSpec, convert it to the corresponding specific case class
   * used within Express.
   *
   * @param spec is the avro input specification.
   * @return the specific case class for the specification, such as KijiInputSpec.
   */
  def avroInputSpecToInputSpec(spec: AvroInputSpec): InputSpec = {
    val specification: AnyRef = spec.getSpecification
    // TODO(EXP-161): Accept inputs from multiple source types.
    specification match {
      case kijiSpec: AvroKijiInputSpec => KijiInputSpec(kijiSpec)
          .asInstanceOf[InputSpec]
      case _ => throw new ValidationException(
          "Unsupported Input Configuration: " + specification.getClass.toString)
    }
  }

  /**
   * Given a generic AvroOutputSpec, convert it to the corresponding specific case class
   * used within Express.
   *
   * @param spec is the avro output specification.
   * @return the specific case class for the specification, such as KijiOutputSpec.
   */
  def avroOutputSpecToOutputSpec(spec: AvroOutputSpec): OutputSpec = {
    val specification: AnyRef = spec.getSpecification
    // TODO(EXP-161): Accept outputs from multiple source types.
    specification match {
      case kijiSpec: AvroKijiOutputSpec => KijiOutputSpec(kijiSpec)
      case kijiSingleCol: AvroKijiSingleColumnOutputSpec =>
          KijiSingleColumnOutputSpec(kijiSingleCol)
      case _ => throw new ValidationException(
        "Unsupported Output Configuration: " + specification.getClass.toString)
    }
  }

  /**
   * Creates a ModelEnvironment given a JSON string. In the process, all fields are validated.
   *
   * @param json serialized model environment.
   * @return the validated model environment.
   */
  def fromJson(json: String): ModelEnvironment = {
    // Parse the json.
    val avroModelEnvironment: AvroModelEnvironment = FromJson
        .fromJsonString(json, AvroModelEnvironment.SCHEMA$)
        .asInstanceOf[AvroModelEnvironment]
    val protocol = ProtocolVersion
        .parse(avroModelEnvironment.getProtocolVersion)
    val prepareAvro = Option(avroModelEnvironment.getPrepareEnvironment)
    val trainAvro = Option(avroModelEnvironment.getTrainEnvironment)
    val scoreAvro = Option(avroModelEnvironment.getScoreEnvironment)

    // Load the preparer's model environment.
    val prepareEnvironment = prepareAvro.map { prepare =>
      val inputConfig = avroInputSpecToInputSpec(prepare.getInputConfig)
      val outputConfig = avroOutputSpecToOutputSpec(prepare.getOutputConfig)

      new PrepareEnvironment(
        inputConfig = inputConfig,
        outputConfig = outputConfig,
        kvstores = prepare.getKvStores
          .asScala
          .map { avro => KVStore(avro) })
    }

      // Load the trainers's model environment.
    val trainEnvironment = trainAvro.map { train =>
      val inputConfig = avroInputSpecToInputSpec(train.getInputConfig)
      val outputConfig =  avroOutputSpecToOutputSpec(train.getOutputConfig)

      new TrainEnvironment(
          inputConfig = inputConfig,
          outputConfig = outputConfig,
          kvstores = train.getKvStores
              .asScala
              .map { avro => KVStore(avro) })
    }

    // Load the scorer's model environment.
    val scoreEnvironment = scoreAvro.map { score =>
      val inputConfig = avroInputSpecToInputSpec(score.getInputConfig)
      val outputConfig =  avroOutputSpecToOutputSpec(score.getOutputConfig)

      new ScoreEnvironment(
          inputConfig = inputConfig,
          outputConfig = outputConfig,
          kvstores = score.getKvStores
              .asScala
              .map { avro => KVStore(avro) })
    }

    // Build a model environment.
    new ModelEnvironment(
        name = avroModelEnvironment.getName,
        version = avroModelEnvironment.getVersion,
        prepareEnvironment = prepareEnvironment,
        trainEnvironment = trainEnvironment,
        scoreEnvironment = scoreEnvironment,
        protocolVersion = protocol)
  }

  /**
   * Creates an ModelEnvironment given a path in the local filesystem to a JSON file that specifies
   * a run specification. In the process, all fields are validated.
   *
   * @param path in the local filesystem to a JSON file containing a model environment.
   * @return the validated model environment.
   */
  def fromJsonFile(path: String): ModelEnvironment = {
    val json: String = doAndClose(Source.fromFile(path)) { source =>
      source.mkString
    }

    fromJson(json)
  }

  /**
   * Converts an Avro data request to a Kiji data request.
   *
   * @param avroDataRequest to convert.
   * @return a Kiji data request converted from the provided Avro data request.
   */
  private[express] def avroToKijiDataRequest(avroDataRequest: AvroDataRequest): KijiDataRequest = {
    val builder = KijiDataRequest.builder()
        .withTimeRange(avroDataRequest.getMinTimestamp(), avroDataRequest.getMaxTimestamp())

    // Add columns to the datarequest.
    avroDataRequest
        .getColumnDefinitions
        .asScala
        .foreach { columnSpec: ColumnSpec =>
          val name = new KijiColumnName(columnSpec.getName())
          val maxVersions = columnSpec.getMaxVersions()
          if (Option(columnSpec.getFilter).isDefined) {
            val filter = ExpressDataRequest
                .filterFromAvro(columnSpec.getFilter)
                .getKijiColumnFilter()
            builder.newColumnsDef().withMaxVersions(maxVersions).withFilter(filter).add(name)
          } else {
            builder.newColumnsDef().withMaxVersions(maxVersions).add(name)
          }
        }

    builder.build()
  }

  /**
   * Verifies that all fields in a model environment are valid. This validation method will collect
   * all validation errors into one exception.
   *
   * @param environment to validate.
   * @throws a ModelEnvironmentValidationException if there are errors encountered while validating
   *     the provided model definition.
   */
  def validateModelEnvironment(environment: ModelEnvironment) {
    // Collect errors from other validation steps.
    val baseErrors: Seq[Option[ValidationException]] = Seq(
        validateProtocolVersion(environment.protocolVersion),
        validateName(environment.name),
        validateVersion(environment.version)
    )

    val prepareErrors = environment.prepareEnvironment.map { env =>
      validatePrepareEnv(env)
    }.flatten
    val trainErrors = environment.trainEnvironment.map { env =>
      validateTrainEnv(env)
    }.flatten
    val scoreErrors = environment.scoreEnvironment.map { env =>
      validateScoreEnv(env)
    }.flatten

    // Throw an exception if there were any validation errors.
    val allErrors = baseErrors ++ prepareErrors ++ trainErrors ++ scoreErrors
    val causes = allErrors.flatten
    if (!causes.isEmpty) {
      throw new ModelEnvironmentValidationException(causes)
    }
  }

  /**
   * Verifies that a model environment's protocol version is supported.
   *
   * @param protocolVersion to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     protocol version.
   */
  def validateProtocolVersion(protocolVersion: ProtocolVersion): Option[ValidationException] = {
    if (MAX_RUN_ENV_VER.compareTo(protocolVersion) < 0) {
      val error = "\"%s\" is the maximum protocol version supported. ".format(MAX_RUN_ENV_VER) +
          "The provided model environment is of protocol version: \"%s\"".format(protocolVersion)
      Some(new ValidationException(error))
    } else if (MIN_RUN_ENV_VER.compareTo(protocolVersion) > 0) {
      val error = "\"%s\" is the minimum protocol version supported. ".format(MIN_RUN_ENV_VER) +
          "The provided model environment is of protocol version: \"%s\"".format(protocolVersion)
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Verifies that a model environment's name is valid.
   *
   * @param name to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     name of the model environment.
   */
  def validateName(name: String): Option[ValidationException] = {
    if(name.isEmpty) {
      val error = "The name of the model environment cannot be the empty string."
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
   * Verifies that a model environment's version string is valid.
   *
   * @param version to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     version string.
   */
  def validateVersion(version: String): Option[ValidationException] = {
    if (!version.matches(VERSION_REGEX)) {
      val error = "Model environment version strings must match the regex " +
          "\"%s\" (1.0.0 would be valid).".format(VERSION_REGEX)
      Some(new ValidationException(error))
    }
    else {
      None
    }
  }

  /**
   * Verifies that the given sequence of FieldBindings is valid with respect to the field names and
   * column names contained therein.
   *
   * @param fieldBindings are the associations between field names and Kiji column names.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateKijiInputOutputFieldBindings(fieldBindings: Seq[FieldBinding]):
      Seq[Option[ValidationException]] = {
    val fieldNames: Seq[String] = fieldBindings.map {
      fieldBinding: FieldBinding => fieldBinding.tupleFieldName
    }
    val columnNames: Seq[String] = fieldBindings.map {
      fieldBinding: FieldBinding => fieldBinding.storeFieldName
    }
    columnNames.map {
      columnName: String => validateKijiColumnName(columnName)
    } ++ Seq(validateFieldNames(fieldNames), validateColumnNames(columnNames))
  }

  /**
   * Verifies that the InputSpec of the train or prepare phase is valid.
   *
   * @param inputSpec to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateInputSpec(inputSpec: InputSpec): Seq[Option[ValidationException]] = {
    inputSpec match {
      case kijiInputSpec: KijiInputSpec => {
        validateKijiInputOutputFieldBindings(kijiInputSpec.fieldBindings) ++
          validateDataRequest(kijiInputSpec.dataRequest)
      }
      // TODO(EXP-161): Accept inputs from multiple source types.
      case _ => Seq(Some(new ValidationException(
        "Unsupported InputSpec type: %s".format(inputSpec.getClass))))
    }
  }

  /**
   * Verifies that the OutputSpec of the train or prepare phase is valid.
   *
   * @param outputSpec to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateOutputSpec(outputSpec: OutputSpec): Seq[Option[ValidationException]] = {
    outputSpec match {
      case kijiOutputSpec: KijiOutputSpec => {
        validateKijiInputOutputFieldBindings(kijiOutputSpec.fieldBindings)
      }
      case kijiSingleColSpec: KijiSingleColumnOutputSpec => {
        Seq(validateKijiColumnName(kijiSingleColSpec.outputColumn))
      }
      // TODO(EXP-161): Accept outputs from multiple source types.
      case _ => Seq(Some(new ValidationException(
        "Unsupported OutputSpec type: %s".format(outputSpec.getClass))))
    }
  }

  /**
   * Verifies that a model environment's prepare phase is valid.
   *
   * @param prepareEnv to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     prepare phase.
   */
  def validatePrepareEnv(prepareEnv: PrepareEnvironment): Seq[Option[ValidationException]] = {
    val inputConfigExcep = validateInputSpec(prepareEnv.inputConfig)

    val outputConfigExcep = validateOutputSpec(prepareEnv.outputConfig)

    val kvStoreExcep = validateKvStores(prepareEnv.kvstores)

    outputConfigExcep ++ inputConfigExcep ++ kvStoreExcep
  }

  /**
   * Verifies that a model environment's train phase is valid.
   *
   * @param trainEnv to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     prepare phase.
   */
  def validateTrainEnv(trainEnv: TrainEnvironment): Seq[Option[ValidationException]] = {
    val inputConfigExcep = validateInputSpec(trainEnv.inputConfig)

    val outputConfigExcep = validateOutputSpec(trainEnv.outputConfig)

    val kvStoreExcep = validateKvStores(trainEnv.kvstores)

    outputConfigExcep ++ inputConfigExcep ++ kvStoreExcep
  }

  /**
   * Verifies that a model environment's score phase is valid.
   *
   * @param scoreEnv to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     score phase.
   */
  def validateScoreEnv(scoreEnv: ScoreEnvironment): Seq[Option[ValidationException]] = {
    val inputConfigExcep = validateInputSpec(scoreEnv.inputConfig)

    // For score, we only permit output to a single Kiji column. Hence we need to validate
    // this as a special case.
    val outputConfigExcep = scoreEnv.outputConfig match {
      case scorePhaseOutputSpec: KijiSingleColumnOutputSpec => {
        Seq(validateKijiColumnName(scorePhaseOutputSpec.outputColumn))
      }
      case _ => Seq(Some(new ValidationException(
        "Unsupported OutputSpec type for Score Phase: %s".format(scoreEnv.outputConfig.getClass))))
    }

    val kvStoreExcep = validateKvStores(scoreEnv.kvstores)

    inputConfigExcep ++ outputConfigExcep ++ kvStoreExcep
  }

  /**
   * Validates a data request. Currently this only validates the column names.
   *
   * @param dataRequest to validate.
   * @throws a ModelEnvironmentValidationException if the column names are invalid.
   */
  def validateDataRequest(dataRequest: ExpressDataRequest): Seq[Option[ValidationException]] = {
    val kijiColNameErrors: Seq[Option[ValidationException]] = dataRequest
        .columnRequests
        .map { column: ExpressColumnRequest =>
          validateKijiColumnName(column.name)
        }
    val minTSError: Option[ValidationException] = if (dataRequest.minTimeStamp < 0) {
      val error = "minTimeStamp in the DataRequest is "+ dataRequest.minTimeStamp +
          " and must be greater than 0"
      Some(new ValidationException(error))
    } else {
      None
    }
    val maxTSError: Option[ValidationException] = if (dataRequest.maxTimeStamp < 0) {
      val error = "maxTimeStamp in the DataRequest is "+ dataRequest.minTimeStamp +
        " and must be greater than 0"
      Some(new ValidationException(error))
    } else {
      None
    }
    kijiColNameErrors :+ minTSError :+ maxTSError
  }

  /**
   * Validate the name of a KVStore.
   *
   * @param kvstore whose name should be validated.
   * @return an optional ValidationException if the name provided is invalid.
   */
  def validateKvstoreName(kvstore: KVStore): Option[ValidationException] = {
    if (!kvstore.name.matches("^[a-zA-Z_][a-zA-Z0-9_]+$")) {
      val error = "The key-value store name must begin with a letter" +
          " and contain only alphanumeric characters and underscores. The key-value store" +
          " name you provided is " + kvstore.name + " and the regex it must match is " +
          "^[a-zA-Z_][a-zA-Z0-9_]+$"
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validate properties specified to initialize the key-value store. Each type of key-value store
   * requires different properties.
   *
   * @param kvstore whose properties to validate.
   * @return an optional ValidationException if any of the properties are invalid.
   */
  def validateKvstoreProperties(kvstore: KVStore): Option[ValidationException] = {
    val properties: Map[String, String] = kvstore.properties

    KvStoreType.valueOf(kvstore.storeType) match {
      case KvStoreType.AVRO_KV => {
        if (!properties.contains("path")) {
          val error = "To use an Avro key-value record key-value store, you must specify the" +
              " HDFS path to the Avro container file to use to back the store. Use the" +
              " property name 'path' to provide the path."
          return Some(new ValidationException(error))
        }
      }

      case KvStoreType.AVRO_RECORD => {
        // Construct an error message for a missing path, missing key_field, or both.
        val pathError =
          if (!properties.contains("path")) {
            "To use an Avro record key-value store, you must specify the HDFS path" +
                " to the Avro container file to use to back the store. Use the property name" +
                " 'path' to provide the path."
          } else {
            ""
          }

        val keyFieldError =
          if (!properties.contains("key_field")) {
            "To use an Avro record key-value store, you must specify the name" +
                " of a record field whose value should be used as the record's key. Use the" +
                " property name 'key_field' to provide the field name."
          } else {
            ""
          }

        val errorMessage = pathError + keyFieldError
        return Some(new ValidationException(errorMessage))
      }

      case KvStoreType.KIJI_TABLE => {
        if (!properties.contains("uri")) {
          val error = "To use a Kiji table key-value store, you must specify a Kiji URI" +
              " addressing the table to use to back the store. Use the property name 'url' to" +
              " provide the URI."
          return Some(new ValidationException(error))
        }
        if (!properties.contains("column")) {
          val error = "To use a Kiji table key-value store, you must specify the qualified-name" +
              " of the column whose most recent value will be used as the value associated with" +
              " each entity id. Use the property name 'column' to specify the column name."
          return Some(new ValidationException(error))
        }
      }

      case kvstoreType => {
        val error = "An unknown key-value store type was specified: " + kvstoreType.toString
        return Some(new ValidationException(error))
      }
    }

    return None
  }

  /**
   * Validates all properties of Seq of KVStores.
   *
   * @param kvstores to validate
   * @return validation exceptions generated while validate the KVStore specification.
   */
  def validateKvStores(kvstores: Seq[KVStore]): Seq[Option[ValidationException]] = {
    // Collect KVStore errors.
    val kvstoreNameErrors: Seq[Option[ValidationException]] = kvstores.map {
      kvstore: KVStore => validateKvstoreName(kvstore)
    }
    val kvstorePropertiesErrors: Seq[Option[ValidationException]] = kvstores.map {
      kvstore: KVStore => validateKvstoreProperties(kvstore)
    }
    kvstoreNameErrors ++ kvstorePropertiesErrors
  }

  /**
   * Validates that the tuple field names are distinct and mapped to a unique column name.
   *
   * @param fieldNames to validate.
   * @return an optional ValidationException if the tuple field names are invalid.
   */
  def validateFieldNames(fieldNames: Seq[String]): Option[ValidationException] = {
    if (fieldNames.distinct.size != fieldNames.size) {
      val duplicates = fieldNames.toList.filterNot(x => fieldNames.toList.distinct == x)
      val error = "Every tuple field name must map to a unique column name. You have" +
          " one or more field names that are associated with" +
          " multiple columns: %s.".format(duplicates.mkString(" "))
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validates that the supplied column names are distinct and mapped to a unique tuple field.
   *
   * @param columnNames to validate.
   * @return an optional ValidationException if the column names are invalid.
   */
  def validateColumnNames(columnNames: Seq[String]): Option[ValidationException] = {
    if (columnNames.distinct.size != columnNames.size) {
      val duplicates = columnNames.toList.filterNot(x => columnNames.toList.distinct == x)
      val error = "Every column name must map to a unique tuple field. You have a data " +
          "field associated with multiple columns: %s".format(duplicates.mkString(" "))
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validates the specified output column for the score phase.
   *
   * @param outputColumn to validate.
   * @return an optional ValidationException if the score output column is invalid.
   */
  def validateScoreOutputColumn(outputColumn: String): Option[ValidationException] = {
    if (outputColumn.isEmpty) {
      val error = "The column to write out to in the score phase cannot be the empty string."
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validates a column name specified in the model environment is can be used to construct a
   * KijiColumnName.
   *
   * @param name is the string representation of the column name.
   * @return an optional ValidationException if the name is invalid.
   */
  def validateKijiColumnName(name: String): Option[ValidationException] = {
    try {
      new KijiColumnName(name)
    } catch {
      case _: KijiInvalidNameException => {
        val error = "The column name " + name + " is invalid."
        return Some(new ValidationException(error))
      }
    }
    None
  }
}
