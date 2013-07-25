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

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source
import scala.Some

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.avro._
import org.kiji.express.util.Resources.doAndClose
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.KijiNameValidator
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson

/**
 * A case class wrapper around the parameters necessary for an Avro FieldBinding.  This is a
 * convenience for users to define FieldBindings when using the ModelEnvironment.
 *
 * @param tupleFieldName is the name of the tuple field to associate with the `storeFieldName`.
 * @param storeFieldName is the name of the store field to associate with the `tupleFieldName`.
 */
case class FieldBinding(tupleFieldName: String, storeFieldName: String) {
  private[express] def toAvroFieldBinding(): AvroFieldBinding = {
    new AvroFieldBinding(tupleFieldName, storeFieldName)
  }
}

/**
 * A case class wrapper around the parameters necessary for an Avro KVStore.  This is a convenience
 * for users to define their KVStores when using the ModelEnvironment.
 *
 * @param storeType of this KVStore.  Must be one of "AVRO_KV", "AVRO_RECORD", "KIJI_TABLE".
 * @param name specified by the user as a shorthand identifier for this KVStore.
 * @param properties that may be needed to configure and instantiate a kv store reader.
 */
case class KVStore(storeType: String, name: String, properties: Map[String, String]) {
  // The allowed store types.
  val possibleStoreTypes = Set("AVRO_KV", "AVRO_RECORD", "KIJI_TABLE")

  require(
      possibleStoreTypes.contains(storeType),
      "storeType must be one of %s, instead was %s".format(possibleStoreTypes, storeType))

  /**
   * Creates an Avro KVStore from this specification.
   *
   * @return an [[org.kiji.express.avro.AvroKVStore]] with its parameters from this specification.
   */
  private[express] def toAvroKVStore(): AvroKVStore = {
    val kvStoreType: KvStoreType = Enum.valueOf(classOf[KvStoreType], storeType)
    val avroProperties: java.util.List[Property] =
        properties.map { case (name, value) =>
            new Property(name, value)
        }.toSeq.asJava
    return new AvroKVStore(kvStoreType, name, avroProperties)
  }
}

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
 * val myDataRequest: ExpressDataRequest = new ExpressDataRequest(0, 38475687,
 *     new ExpressColumnRequest("info:in", 3, None)::Nil)
 * val extractEnv = ExtractEnvironment(
 *     dataRequest = myDataRequest,
 *     fieldBindings = Seq(FieldBinding("tuplename", "storefieldname")),
 *     kvstores = Seq(KVStore("AVRO_KV", "storename", Map())))
 * val scoreEnv2 = ScoreEnvironment(
 *     outputColumn = "outputFamily:qualifier",
 *     kvstores = Seq(KVStore("KIJI_TABLE", "myname", Map())))
 * val modelEnv = ModelEnvironment(
 *   name = "myname",
 *   version = "1.0.0",
 *   modelTableUri = "kiji://myuri",
 *   extractEnvironment = extractEnv,
 *   scoreEnvironment = scoreEnv)
 * }}}
 *
 * Alternatively a ModelEnvironment can be created from JSON. JSON run specifications should
 * be written using the following format:
 * {{{
 * {
 *   "name" : "myRunProfile",
 *   "version" : "1.0.0",
 *   "model_table_uri" : "kiji://.env/default/mytable",
 *   "protocol_version" : "model_environment-0.1.0",
 *   "extract_model_environment" : {
 *     "data_request" : {
 *       "min_timestamp" : 0,
 *       "max_timestamp" : 922347862,
 *       "column_definitions" : [ {
 *         "name" : "info:in",
 *         "max_versions" : 3
 *       } ]
 *     },
 *     "kv_stores" : [ {
 *       "store_type" : "AVRO_KV",
 *       "name" : "side_data",
 *       "properties" : [ {
 *         "name" : "path",
 *         "value" : "/usr/src/and/so/on"
 *       } ]
 *     } ],
 *     "field_bindings" : [ {
 *       "tuple_field_name" : "in",
 *       "store_field_name" : "info:in"
 *     } ]
 *   },
 *   "score_model_environment" : {
 *     "kv_stores" : [ {
 *       "store_type" : "AVRO_KV",
 *       "name" : "side_data",
 *       "properties" : [ {
 *         "name" : "path",
 *         "value" : "/usr/src/and/so/on"
 *       } ]
 *     } ],
 *     "output_column" : "info:out"
 *   }
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
 * @param modelTableUri is the URI of the Kiji table this model environment will read from and
 *     write to.
 * @param prepareEnvironment defining configuration details specific to the Prepare phase of
 *     a model.
 * @param extractEnvironment defining configuration details specific to the Extract phase of
 *     a model.
 * @param scoreEnvironment defining configuration details specific to the Score phase of a
 *     model.
 * @param protocolVersion this model definition was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ModelEnvironment private[express] (
    val name: String,
    val version: String,
    val modelTableUri: String,
    val prepareEnvironment: PrepareEnvironment,
    val extractEnvironment: ExtractEnvironment,
    val scoreEnvironment: ScoreEnvironment,
    private[express] val protocolVersion: ProtocolVersion =
        ModelEnvironment.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model environment are valid.
  ModelEnvironment.validateModelEnvironment(this)


  /**
   * TODO(EXP-164): Allow the prepare phase to be optional.
   */

  /**
   * Serializes this model environment into a JSON string.
   *
   * @return a JSON string that represents this model environment.
   */
  final def toJson(): String = {
    // Build an AvroPrepareEnvironmentRecord.
    val avroPrepareEnvironment: AvroPrepareEnvironment = AvroPrepareEnvironment
        .newBuilder()
        .setFieldBindings(prepareEnvironment.fieldBindings.asJava)
        .setDataRequest(prepareEnvironment.dataRequest.toAvro)
        .setKvStores(prepareEnvironment.kvstores.map { kvstore => kvstore.toAvroKVStore }.asJava)
        .setOutputColumn(prepareEnvironment.outputColumn)
        .build()

    // Build an AvroExtractEnvironment record.
    val avroExtractEnvironment: AvroExtractEnvironment = AvroExtractEnvironment
        .newBuilder()
        .setDataRequest(extractEnvironment.dataRequest.toAvro())
        .setKvStores(extractEnvironment.kvstores.map { kvstore => kvstore.toAvroKVStore }.asJava)
        .setFieldBindings(extractEnvironment.fieldBindings.asJava)
        .build()

    // Build an AvroScoreEnvironment record.
    val avroScoreEnvironment: AvroScoreEnvironment = AvroScoreEnvironment
        .newBuilder()
        .setKvStores(scoreEnvironment.kvstores.map { storeSpec => storeSpec.toAvroKVStore }.asJava)
        .setOutputColumn(scoreEnvironment.outputColumn)
        .build()

    // Build an AvroModelEnvironment record.
    val environment: AvroModelEnvironment = AvroModelEnvironment
        .newBuilder()
        .setName(name)
        .setVersion(version)
        .setProtocolVersion(protocolVersion.toString)
        .setModelTableUri(modelTableUri)
        .setPrepareEnvironment(avroPrepareEnvironment)
        .setExtractEnvironment(avroExtractEnvironment)
        .setScoreEnvironment(avroScoreEnvironment)
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
   * @param modelTableUri is the URI of the Kiji table this model environment will read from and
   *     write to.
   * @param prepareEnvironment defining configuration details specific to the Prepare phase of
   *     a model.
   * @param extractEnvironment defining configuration details specific to the Extract phase of
   *     a model.
   * @param scoreEnvironment defining configuration details specific to the Score phase of a
   *     model.
   */
  final def withNewSettings(
      name: String = this.name,
      version: String = this.version,
      modelTableUri: String = this.modelTableUri,
      prepareEnvironment: PrepareEnvironment = this.prepareEnvironment,
      extractEnvironment: ExtractEnvironment = this.extractEnvironment,
      scoreEnvironment: ScoreEnvironment = this.scoreEnvironment): ModelEnvironment = {
    new ModelEnvironment(
        name,
        version,
        modelTableUri,
        prepareEnvironment,
        extractEnvironment,
        scoreEnvironment)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case environment: ModelEnvironment => {
        name == environment.name &&
            version == environment.version &&
            modelTableUri == environment.modelTableUri &&
            prepareEnvironment == environment.prepareEnvironment &&
            extractEnvironment == environment.extractEnvironment &&
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
          modelTableUri,
          prepareEnvironment,
          extractEnvironment,
          scoreEnvironment,
          protocolVersion)
}

/**
 * Companion object for ModelEnvironment. Contains constants related to model environments as well
 * as validation methods.
 */
object ModelEnvironment {
  /** Maximum model environment version we can recognize. */
  val MAX_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.1.0")

  /** Minimum model environment version we can recognize. */
  val MIN_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.1.0")

  /** Current ModelDefinition protocol version. */
  val CURRENT_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.1.0")

  /** Regular expression used to validate a model environment version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model definition. */
  private[express] val VALIDATION_MESSAGE = "One or more errors occurred while validating your" +
      " model environment. Please correct the problems in your model environment and try again."

  /**
   * Creates a new ModelEnvironment using the specified settings.
   * @param name of the model environment.
   * @param version of the model environment.
   * @param modelTableUri is the URI of the Kiji table this model environment will read from and
   *     write to.
   * @param prepareEnvironment defining configuration details specific to the Prepare phase of
   *     a model.
   * @param extractEnvironment defining configuration details specific to the Extract phase of
   *     a model.
   * @param scoreEnvironment defining configuration details specific to the Score phase of a
   *     model.
   * @return a model environment with the specified settings.
   */
  def apply(
      name: String,
      version: String,
      modelTableUri: String,
      prepareEnvironment: PrepareEnvironment,
      extractEnvironment: ExtractEnvironment,
      scoreEnvironment: ScoreEnvironment): ModelEnvironment = {
    new ModelEnvironment(name, version, modelTableUri, prepareEnvironment, extractEnvironment,
        scoreEnvironment)
  }

  /**
   * Converts an Avro KVStore specification into a KVStore case class.
   * @param avroKVStore is the Avro specification.
   * @return the KVStore specification as a KVStore case class.
   */
  private def avroKVStoreToKVStore(avroKVStore: AvroKVStore): KVStore = {
    return KVStore(
        storeType=avroKVStore.getStoreType.toString,
        name=avroKVStore.getName,
        properties=propertiesToMap(avroKVStore.getProperties)
    )
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
    val extractAvroDataRequest = avroModelEnvironment.getExtractEnvironment.getDataRequest
    val prepareAvroDataRequest = avroModelEnvironment.getPrepareEnvironment.getDataRequest
    // Validate the Avro data request.

    // Load the preparer's model environment.
    val prepareEnvironment = new PrepareEnvironment(
      dataRequest = ExpressDataRequest(prepareAvroDataRequest),
      fieldBindings = avroModelEnvironment.getPrepareEnvironment.getFieldBindings.asScala,
      kvstores = avroModelEnvironment.getPrepareEnvironment.getKvStores
          .asScala.map { avro => avroKVStoreToKVStore(avro) },
      avroModelEnvironment.getPrepareEnvironment.getOutputColumn)

    // Load the extractor's model environment.
    val extractEnvironment = new ExtractEnvironment(
        dataRequest = ExpressDataRequest(extractAvroDataRequest),
        fieldBindings = avroModelEnvironment.getExtractEnvironment.getFieldBindings.asScala,
        kvstores = avroModelEnvironment.getExtractEnvironment.getKvStores
            .asScala.map { avro => avroKVStoreToKVStore(avro) })

    // Load the scorer's model environment.
    val scoreEnvironment = new ScoreEnvironment(
        outputColumn = avroModelEnvironment.getScoreEnvironment.getOutputColumn,
        kvstores = avroModelEnvironment.getScoreEnvironment.getKvStores
            .asScala.map { avro => avroKVStoreToKVStore(avro) })

    // Build a model environment.
    new ModelEnvironment(
        name = avroModelEnvironment.getName,
        version = avroModelEnvironment.getVersion,
        modelTableUri = avroModelEnvironment.getModelTableUri,
        prepareEnvironment = prepareEnvironment,
        extractEnvironment = extractEnvironment,
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
          if (columnSpec.getFilter != null) {
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

    val prepareErrors = validatePrepareEnv(environment.prepareEnvironment)
    val extractErrors = validateExtractEnv(environment.extractEnvironment)
    val scoreErrors = validateScoreEnv(environment.scoreEnvironment)

    // Throw an exception if there were any validation errors.
    val allErrors = baseErrors ++ prepareErrors ++ extractErrors ++ scoreErrors
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
   * Verifies that a model environments's prepare phase is valid.
   *
   * @param prepareEnv to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     prepare phase.
   */
  def validatePrepareEnv(prepareEnv: PrepareEnvironment): Seq[Option[ValidationException]] = {
    val fieldNames: Seq[String] = prepareEnv.fieldBindings.map {
      fieldBinding: AvroFieldBinding => fieldBinding.getTupleFieldName
    }
    val columnNames: Seq[String] = prepareEnv.fieldBindings.map {
      fieldBinding: AvroFieldBinding => fieldBinding.getStoreFieldName
    }
    val fieldBindingExcep = Seq(validateFieldNames(fieldNames), validateColumnNames(columnNames))
    val kvStoreExcep = validateKvStores(prepareEnv.kvstores)
    val dataReqExcep = validateDataRequest(prepareEnv.dataRequest)

    val colNameExcep = Seq(validateKijiColumnName(prepareEnv.outputColumn))

    fieldBindingExcep ++ kvStoreExcep ++ dataReqExcep ++ colNameExcep
  }

  /**
   * Verifies that a model environments's extract phase is valid.
   *
   * @param extractEnv to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     extract phase.
   */
  def validateExtractEnv(extractEnv: ExtractEnvironment): Seq[Option[ValidationException]] = {
    val fieldNames: Seq[String] = extractEnv.fieldBindings.map {
      fieldBinding: AvroFieldBinding => fieldBinding.getTupleFieldName
    }
    val columnNames: Seq[String] = extractEnv.fieldBindings.map {
      fieldBinding: AvroFieldBinding => fieldBinding.getStoreFieldName
    }
    val fieldBindingExcep = Seq(validateFieldNames(fieldNames), validateColumnNames(columnNames))
    val kvStoreExcep = validateKvStores(extractEnv.kvstores)
    val dataReqExcep = validateDataRequest(extractEnv.dataRequest)

    fieldBindingExcep ++ kvStoreExcep ++ dataReqExcep
  }

  /**
   * Verifies that a model environments's score phase is valid.
   *
   * @param scoreEnv to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     score phase.
   */
  def validateScoreEnv(scoreEnv: ScoreEnvironment): Seq[Option[ValidationException]] = {
    val colNameExcep = Seq(validateKijiColumnName(scoreEnv.outputColumn))
    val kvStoreExcep = validateKvStores(scoreEnv.kvstores)

    colNameExcep ++ kvStoreExcep
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
    kijiColNameErrors
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

  /**
   * Converts a Java list of Avro Property into a Scala map.
   * @param properties is a Java list of Avro Property.
   * @return the properties, as a Scala map.
   */
  private def propertiesToMap(properties: java.util.List[Property]): Map[String, String] = {
    return properties.asScala.map { prop => (prop.getName, prop.getValue) }.toMap
  }
}
