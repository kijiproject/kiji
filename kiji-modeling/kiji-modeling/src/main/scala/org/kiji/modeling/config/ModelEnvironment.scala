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
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.util.Resources.doAndClose
import org.kiji.modeling.avro.AvroModelEnvironment
import org.kiji.modeling.framework.ModelConverters
import org.kiji.schema.KijiColumnName
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
 *   val modelEnv = ModelEnvironment(
 *       name = "myname",
 *       version = "1.0.0",
 *       prepareEnvironment = None,
 *       trainEnvironment = None,
 *       scoreEnvironment = Some(ScoreEnvironment(
 *           inputSpec = KijiInputSpec(
 *               tableUri = "kiji://.env/default/mytable/",
 *               timeRange = Between(0L, 38475687L),
 *               columnsToFields = Map(
 *                   QualifiedColumnInputSpec("info", "in", maxVersions = 3) -> 'tuplename)),
 *           outputSpec = KijiSingleColumnOutputSpec(
 *               tableUri = "kiji://.env/default/mytable/",
 *               outputColumn = ColumnOutputSpec("outputFamily:qualifier")),
 *           keyValueStoreSpecs = Seq(
 *               KeyValueStore(
 *                   storeType = "KIJI_TABLE",
 *                   name = "myname",
 *                   properties = Map(
 *                       "uri" -> "kiji://.env/default/table",
 *                       "column" -> "info:email")),
 *               KeyValueStore(
 *                   storeType = "AVRO_KV",
 *                   name = "storename",
 *                   properties = Map(
 *                       "path" -> "/some/great/path"))))))
 * }}}
 *
 * Alternatively a ModelEnvironment can be created from JSON. JSON run specifications should
 * be written using the following format:
 *
 *{{{
 *{
 *  "protocol_version":"model_environment-0.3.0",
 *  "name":"myRunProfile",
 *  "version":"1.0.0",
 *  "prepare_environment":{
 *    "org.kiji.modeling.avro.AvroPrepareEnvironment":{
 *      "input_spec":{
 *        "kijitableinput" : {
 *          "kiji_specification":{
 *            "org.kiji.modeling.avro.AvroKijiInputSpec":{
 *              "table_uri":"kiji://.env/default/table",
 *              "time_range" : { "min_timestamp" : 0, "max_timestamp" : 38475687 },
 *              "columns_to_fields" : [ {
 *                "tuple_field_name" : "tuplename",
 *                "column" : { "org.kiji.modeling.avro.AvroQualifiedColumnInputSpec" : {
 *                  "family" : "info",
 *                  "qualifier" : "in",
 *                  "max_versions" : 3,
 *                  "filter":{
 *                    "org.kiji.modeling.avro.AvroFilter":{
 *                      "regex_filter":{
 *                        "org.kiji.modeling.avro.AvroRegexQualifierFilter":{
 *                          "regex":"foo"
 *                        }
 *                      }
 *                    }
 *                  }
 *                }}
 *              }]
 *            }
 *          }
 *        }
 *      },
 *      "output_spec":{
 *        "kijitableoutput" : {
 *          "kiji_specification":{
 *            "org.kiji.modeling.avro.AvroKijiOutputSpec":{
 *              "table_uri":"kiji://myuri",
 *              "fields_to_columns" : [ {
 *                "tuple_field_name" : "outputtuple",
 *                "column" : { "org.kiji.modeling.avro.AvroQualifiedColumnOutputSpec" : {
 *                  "family" : "info",
 *                  "qualifier" : "out"
 *                }}
 *              }]
 *            }
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
 *  }}
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
 * @param evaluateEnvironment defining configuration details specific to the Evaluate phase of a
 *     model. Optional.
 * @param protocolVersion this model definition was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ModelEnvironment(
    name: String,
    version: String,
    prepareEnvironment: Option[PrepareEnvironment] = None,
    trainEnvironment: Option[TrainEnvironment] = None,
    scoreEnvironment: Option[ScoreEnvironment] = None,
    evaluateEnvironment: Option[EvaluateEnvironment] = None,
    private[kiji] val protocolVersion: ProtocolVersion =
        ModelEnvironment.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model environment are valid.
  ModelEnvironment.validateModelEnvironment(this)

  /**
   * Serializes this model environment into a JSON string.
   *
   * @return a JSON string that represents this model environment.
   */
  def toJson: String = {
    val environment: AvroModelEnvironment = ModelConverters.modelEnvironmentToAvro(this)

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
   * @param evaluateEnvironment defining configuration details specific to the Evaluate phase of a
   *     model.
   */
  def withNewSettings(
      name: String = this.name,
      version: String = this.version,
      prepareEnvironment: Option[PrepareEnvironment] = this.prepareEnvironment,
      trainEnvironment: Option[TrainEnvironment] = this.trainEnvironment,
      scoreEnvironment: Option[ScoreEnvironment] = this.scoreEnvironment,
      evaluateEnvironment: Option[EvaluateEnvironment] = this.evaluateEnvironment
  ): ModelEnvironment = {
    new ModelEnvironment(
        name,
        version,
        prepareEnvironment,
        trainEnvironment,
        scoreEnvironment,
        evaluateEnvironment)
  }
}

/**
 * Companion object for ModelEnvironment. Contains constants related to model environments as well
 * as validation methods.
 */
object ModelEnvironment {
  /** Maximum model environment version we can recognize. */
  val MAX_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.4.0")

  /** Minimum model environment version we can recognize. */
  val MIN_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.4.0")

  /** Current ModelDefinition protocol version. */
  val CURRENT_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.4.0")

  /** Regular expression used to validate a model environment version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model definition. */
  private[kiji] val VALIDATION_MESSAGE: String = "One or more errors occurred while" +
      " validating your model environment. Please correct the problems in your model environment" +
      " and try again."

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

    // Build a model environment.
    ModelConverters.modelEnvironmentFromAvro(avroModelEnvironment)
  }

  /**
   * Creates an ModelEnvironment given a path in the local filesystem to a JSON file that specifies
   * a run specification. In the process, all fields are validated.
   *
   * @param path in the local filesystem to a JSON file containing a model environment.
   * @return the validated model environment.
   */
  def fromJsonFile(path: String): ModelEnvironment = {
    val json: String = doAndClose(Source.fromFile(path)) { source => source.mkString }

    fromJson(json)
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
    val baseErrors: Seq[ValidationException] =
        validateProtocolVersion(environment.protocolVersion).toSeq ++
        validateName(environment.name) ++
        validateVersion(environment.version)

    val prepareErrors: Seq[ValidationException] = environment
        .prepareEnvironment
        .map { env => validatePrepareEnv(env) }
        .getOrElse(Seq())
    val trainErrors: Seq[ValidationException] = environment
        .trainEnvironment
        .map { env => validateTrainEnv(env) }
        .getOrElse(Seq())
    val scoreErrors: Seq[ValidationException] = environment
        .scoreEnvironment
        .map { env => validateScoreEnv(env) }
        .getOrElse(Seq())
    val evaluateErrors: Seq[ValidationException] = environment
        .evaluateEnvironment
        .map { env => validateEvaluateEnv(env) }
        .getOrElse(Seq())

    // Throw an exception if there were any validation errors.
    val causes = baseErrors ++ prepareErrors ++ trainErrors ++ scoreErrors ++ evaluateErrors
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
   *         name of the model environment.
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
   * Verifies that the given collection of bindings between Scalding field names and column output
   * spec is valid with respect to the field names and column names contained therein.
   *
   * @param columns is a map from field names to [[org.kiji.express.flow.ColumnOutputSpec]] objects.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateKijiOutputFieldBindings(
      columns: Map[Symbol, _ <: ColumnOutputSpec]): Seq[ValidationException] = {
    // Validate column names.
    val columnNames: Seq[String] = columns
        .values
        .toList
        .map { _.columnName.toString }
    val columnNameErrors = columnNames
        .map { columnName: String => validateKijiColumnName(columnName) }
        .flatten

    // Validate field name bindings.
    val fieldNames: Seq[String] = columns
        .keys
        .toList
        .map { field: Symbol => field.name }
    val fieldNameErrors = validateFieldNames(fieldNames).toSeq ++ validateColumnNames(columnNames)

    columnNameErrors ++ fieldNameErrors
  }

  /**
   * Wrapper function for validating the input specification map for the prepare and train phase.
   *
   * @param inputSpecs the map of [[org.kiji.modeling.config.InputSpec]] to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateInputSpecs(inputSpecs: Map[String, InputSpec]): Seq[ValidationException] = {
    inputSpecs.mapValues(spec => validateInputSpec(spec)).values.toSeq.flatten
  }

  /**
   * Verifies that the InputSpec of the train or prepare phase is valid.
   *
   * @param inputSpec to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateInputSpec(inputSpec: InputSpec): Seq[ValidationException] = {
    inputSpec match {
      case kijiInputSpec: KijiInputSpec => {
        validateKijiInputSpec(kijiInputSpec)
      }
      case textSpecification: TextSourceSpec => {
        Seq()
      }
      case SequenceFileSourceSpec(_, keyFieldOption, valueFieldOption) => {
        if (keyFieldOption.isEmpty == valueFieldOption.isEmpty) {
          val error = "Both a key field and a value field must be specified."

          Seq(new ValidationException(error))
        } else {
          Seq()
        }
      }
      case _ => {
        val error = "Unsupported InputSpec type: %s".format(inputSpec.getClass)

        Seq(new ValidationException(error))
      }
    }
  }


  /**
   * Verifies that the KijiInputSpec of the train or prepare phase is valid.
   *
   * @param kijiInputSpec The KijiInputSpec to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateKijiInputSpec(kijiInputSpec: KijiInputSpec): Seq[ValidationException] = {
    // Check the column names
    val nameExceptions = kijiInputSpec
        .columnsToFields
        .keys
        .toList
        .map { _.columnName.toString }
        .map { validateKijiColumnName }
        .flatten

    val beginException: Option[ValidationException] =
        if (kijiInputSpec.timeRange.begin < 0) {
          val error = "timeRange.begin is " + kijiInputSpec.timeRange.begin +
              " but must be greater than zero"
          Some(new ValidationException(error))
        } else {
          None
        }

    val endException: Option[ValidationException] =
        if (kijiInputSpec.timeRange.end < 0) {
          val error = "timeRange.end is " + kijiInputSpec.timeRange.end +
              " but must be greater than zero"
          Some(new ValidationException(error))
        } else {
          None
        }

    nameExceptions ++ beginException ++ endException
  }

  /**
   * Wrapper function for validating the output specification map for the prepare and train phase.
   *
   * @param outputSpecs the map of [[org.kiji.modeling.config.OutputSpec]] to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateOutputSpecs(outputSpecs: Map[String, OutputSpec]): Seq[ValidationException] = {
    outputSpecs.mapValues(spec => validateOutputSpec(spec)).values.toSeq.flatten
  }

  /**
   * Verifies that the OutputSpec of the train or prepare phase is valid.
   *
   * @param outputSpec to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateOutputSpec(outputSpec: OutputSpec): Seq[ValidationException] = {
    outputSpec match {
      case kijiOutputSpec: KijiOutputSpec => {
        validateKijiOutputFieldBindings(kijiOutputSpec.fieldsToColumns)
      }
      case kijiSingleColumnSpec: KijiSingleColumnOutputSpec => {
        validateKijiColumnName(kijiSingleColumnSpec.outputColumn.columnName.toString)
            .toSeq
      }
      case textSpecification: TextSourceSpec => {
        Seq()
      }
      case SequenceFileSourceSpec(_, keyFieldOption, valueFieldOption) => {
        if (keyFieldOption.isEmpty == valueFieldOption.isEmpty) {
          val error = "Both a key field and a value field must be specified."

          Seq(new ValidationException(error))
        } else {
          Seq()
        }
      }
      // TODO(EXP-161): Accept outputs from multiple source types.
      case _ => {
        val error = "Unsupported OutputSpec type: %s".format(outputSpec.getClass)

        Seq(new ValidationException(error))
      }
    }
  }

  /**
   * Verifies that a model environment's prepare phase is valid.
   *
   * @param prepareEnvironment to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     prepare phase.
   */
  def validatePrepareEnv(prepareEnvironment: PrepareEnvironment): Seq[ValidationException] = {
    val inputSpecErrors = validateInputSpecs(prepareEnvironment.inputSpec)
    val outputSpecErrors = validateOutputSpecs(prepareEnvironment.outputSpec)
    val kvStoreErrors = validateKvStores(prepareEnvironment.keyValueStoreSpecs)

    inputSpecErrors ++ outputSpecErrors ++ kvStoreErrors
  }

  /**
   * Verifies that a model environment's train phase is valid.
   *
   * @param trainEnvironment to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     train phase.
   */
  def validateTrainEnv(trainEnvironment: TrainEnvironment): Seq[ValidationException] = {
    val inputSpecErrors = validateInputSpecs(trainEnvironment.inputSpec)
    val outputSpecErrors = validateOutputSpecs(trainEnvironment.outputSpec)
    val kvStoreErrors = validateKvStores(trainEnvironment.keyValueStoreSpecs)

    inputSpecErrors ++ outputSpecErrors ++ kvStoreErrors
  }

  /**
   * Verifies that a model environment's score phase is valid.
   *
   * @param scoreEnvironment to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     score phase.
   */
  def validateScoreEnv(scoreEnvironment: ScoreEnvironment): Seq[ValidationException] = {
    val inputSpecErrors = validateInputSpec(scoreEnvironment.inputSpec)
    // The score phase only supports the single column Kiji output.
    val outputSpecErrors = scoreEnvironment.outputSpec match {
      case scorePhaseOutputSpec: KijiSingleColumnOutputSpec => {
        validateKijiColumnName(scorePhaseOutputSpec.outputColumn.columnName.toString)
            .toSeq
      }
      case _ => {
        val error = "Unsupported OutputSpec type for Score Phase: %s"
            .format(scoreEnvironment.outputSpec.getClass)

        Seq(new ValidationException(error))
      }
    }
    val kvStoreErrors = validateKvStores(scoreEnvironment.keyValueStoreSpecs)

    inputSpecErrors ++ outputSpecErrors ++ kvStoreErrors
  }

  /**
   * Verifies that a model environment's evaluate phase is valid.
   *
   * @param evaluateEnvironment to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     evaluate phase.
   */
  def validateEvaluateEnv(evaluateEnvironment: EvaluateEnvironment): Seq[ValidationException] = {
    val inputSpecErrors = validateInputSpec(evaluateEnvironment.inputSpec)
    val outputSpecErrors = validateOutputSpec(evaluateEnvironment.outputSpec)
    val kvStoreErrors = validateKvStores(evaluateEnvironment.keyValueStoreSpecs)

    inputSpecErrors ++ outputSpecErrors ++ kvStoreErrors
  }

  /**
   * Validate the name of a KVStore.
   *
   * @param keyValueStoreSpec whose name should be validated.
   * @return an optional ValidationException if the name provided is invalid.
   */
  def validateKvstoreName(keyValueStoreSpec: KeyValueStoreSpec): Option[ValidationException] = {
    if (!keyValueStoreSpec.name.matches("^[a-zA-Z_][a-zA-Z0-9_]+$")) {
      val error = "The key-value store name must begin with a letter" +
          " and contain only alphanumeric characters and underscores. The key-value store" +
          " name you provided is " + keyValueStoreSpec.name + " and the regex it must match is " +
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
   * @param keyValueStoreSpec whose properties to validate.
   * @return an optional ValidationException if any of the properties are invalid.
   */
  def validateKvstoreProperties(
      keyValueStoreSpec: KeyValueStoreSpec): Option[ValidationException] = {
    val properties: Map[String, String] = keyValueStoreSpec.properties

    keyValueStoreSpec.storeType match {
      case "AVRO_KV" => {
        if (!properties.contains("path")) {
          val error = "To use an Avro key-value record key-value store, you must specify the" +
              " HDFS path to the Avro container file to use to back the store. Use the" +
              " property name 'path' to provide the path."
          return Some(new ValidationException(error))
        }
      }

      case "AVRO_RECORD" => {
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

      case "KIJI_TABLE" => {
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

      case "TEXT_FILE" => {
        if (!properties.contains("path")) {
          val error = "To use a text-file based store, you must specify the HDFS path to the text" +
              " file to use to back the store. Use the property name 'path' to provide the path."
          return Some(new ValidationException(error))
        }
        if (!properties.contains("delimiter")) {
          val error = "To use a text-file based store, you must specify the text delimiter" +
              " separating keys from values. Use the property name 'delimiter' to specify the" +
              " delimiter."
          return Some(new ValidationException(error))
        }
      }

      case keyValueStoreType => {
        val error = "An unknown key-value store type was specified: " + keyValueStoreType
        return Some(new ValidationException(error))
      }
    }

    return None
  }

  /**
   * Validates all properties of Seq of KVStores.
   *
   * @param keyValueStoreSpecs to validate
   * @return validation exceptions generated while validate the KVStore specification.
   */
  def validateKvStores(keyValueStoreSpecs: Seq[KeyValueStoreSpec]): Seq[ValidationException] = {
    // Collect KVStore errors.
    val nameErrors: Seq[ValidationException] = keyValueStoreSpecs
        .map { keyValueStore: KeyValueStoreSpec => validateKvstoreName(keyValueStore) }
        .flatten
    val propertiesErrors: Seq[ValidationException] = keyValueStoreSpecs
        .map { keyValueStore: KeyValueStoreSpec => validateKvstoreProperties(keyValueStore) }
        .flatten

    nameErrors ++ propertiesErrors
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
