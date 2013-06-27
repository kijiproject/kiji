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

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.io.Source

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.avro.AvroDataRequest
import org.kiji.express.avro.AvroExtractEnvironment
import org.kiji.express.avro.AvroModelEnvironment
import org.kiji.express.avro.AvroScoreEnvironment
import org.kiji.express.avro.ColumnSpec
import org.kiji.express.avro.KVStore
import org.kiji.express.avro.KvStoreType
import org.kiji.express.avro.FieldBinding
import org.kiji.express.avro.Property
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
case class FieldBindingSpec(tupleFieldName: String, storeFieldName: String) {
  def toAvroFieldBinding(): FieldBinding = {
    new FieldBinding(tupleFieldName, storeFieldName)
  }
}

/**
 * A specification of the runtime bindings needed in the extract phase of a model.
 *
 * @param dataRequest describing the input columns required during the extract phase.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how data that is requested for the extract phase is mapped onto named
 *     input fields.
 * @param kvstores for usage during the extract phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ExtractEnvironment private[express] (
    val dataRequest: KijiDataRequest,
    val fieldBindings: Seq[FieldBinding],
    val kvstores: Seq[KVStore]) {
  override def equals(other: Any): Boolean = {
    other match {
      case environment: ExtractEnvironment => {
        dataRequest == environment.dataRequest &&
            fieldBindings == environment.fieldBindings &&
            kvstores == environment.kvstores
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(
          dataRequest,
          fieldBindings,
          kvstores)
}

/**
 * Companion object to ExtractEnvironment containing factory methods.
 */
object ExtractEnvironment {
  /**
   * Creates a new ExtractEnvironment, which is a specification of the runtime bindings needed in
   * the extract phase of a model.
   *
   * @param dataRequest describing the input columns required during the extract phase.
   * @param fieldBindings describing a mapping from columns requested to their corresponding field
   *     names. This determines how data that is requested for the extract phase is mapped onto
   *     named input fields.
   * @param kvstores describing the kv stores for usage during the extract phase.
   * @return an ExtractEnvironment with the configuration specified.
   */
  def apply(
      dataRequest: KijiDataRequest,
      fieldBindings: Seq[FieldBindingSpec],
      kvstores: Seq[KVStoreSpec]): ExtractEnvironment = {
    new ExtractEnvironment(
        dataRequest,
        fieldBindings.map { fieldBindingSpec => fieldBindingSpec.toAvroFieldBinding },
        kvstores.map { kvStoreSpec => kvStoreSpec.toAvroKVStore } )
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
case class KVStoreSpec(storeType: String, name: String, properties: Map[String, String]) {
  // The allowed store types.
  val possibleStoreTypes = Set("AVRO_KV", "AVRO_RECORD", "KIJI_TABLE")

  require(
      possibleStoreTypes.contains(storeType),
      "storeType must be one of %s, instead was %s".format(possibleStoreTypes, storeType))

  /**
   * Creates an Avro KVStore from this specification.
   *
   * @return an [[org.kiji.express.avro.KVStore]] with its parameters from this specification.
   */
  def toAvroKVStore(): KVStore = {
    val kvStoreType: KvStoreType = Enum.valueOf(classOf[KvStoreType], storeType)
    val avroProperties: java.util.List[Property] =
        properties.map { case (name, value) =>
            new Property(name, value)
        }.toSeq.asJava
    new KVStore(kvStoreType, name, avroProperties)
  }
}

/**
 * A specification of the runtime bindings for data sources required in the score phase of a model.
 *
 * @param outputColumn to write scores to.
 * @param kvstores for usage during the score phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ScoreEnvironment private[express] (
    val outputColumn: String,
    val kvstores: Seq[KVStore]) {
  override def equals(other: Any): Boolean = {
    other match {
      case environment: ScoreEnvironment => {
        outputColumn == environment.outputColumn &&
            kvstores == environment.kvstores
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(
          outputColumn,
          kvstores)
}

/**
 * Companion object containing factory methods for ScoreEnvironment.
 */
object ScoreEnvironment {
  /**
   * Creates a ScoreEnvironment, which is a specification of the runtime bindings for data sources
   * required in the score phase of a model.
   *
   * @param outputColumn to write scores to.
   * @param kvstores is the specification of the kv stores for usage during the score phase.
   * @return a ScoreEnvironment with the specified settings.
   */
  def apply(outputColumn: String, kvstores: Seq[KVStoreSpec]): ScoreEnvironment = {
    new ScoreEnvironment(outputColumn, kvstores.map { storeSpec => storeSpec.toAvroKVStore })
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
 * val myDataRequest: KijiDataRequest = {
 *   val builder = KijiDataRequest.builder().withTimeRange(0, 38475687)
 *   builder.newColumnsDef().withMaxVersions(3).add("info", "in")
 *   builder.build()
 * }
 * val extractEnv = ExtractEnvironment(
 *     dataRequest = myDataRequest,
 *     fieldBindings = Seq(FieldBindingSpec("tuplename", "storefieldname")),
 *     kvstores = Seq(KVStoreSpec("AVRO_KV", "storename", Map())))
 * val scoreEnv2 = ScoreEnvironment(
 *     outputColumn = "outputFamily:qualifier",
 *     kvstores = Seq(KVStoreSpec("KIJI_TABLE", "myname", Map())))
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
    val extractEnvironment: ExtractEnvironment,
    val scoreEnvironment: ScoreEnvironment,
    private[express] val protocolVersion: ProtocolVersion =
        ModelEnvironment.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model environment are valid.
  ModelEnvironment.validateModelEnvironment(this)

  /**
   * Serializes this model environment into a JSON string.
   *
   * @return a JSON string that represents this model environment.
   */
  final def toJson(): String = {
    // Build an AvroExtractEnvironment record.
    val avroExtractEnvironment: AvroExtractEnvironment = AvroExtractEnvironment
        .newBuilder()
        .setDataRequest(ModelEnvironment.kijiToAvroDataRequest(extractEnvironment.dataRequest))
        .setKvStores(extractEnvironment.kvstores.asJava)
        .setFieldBindings(extractEnvironment.fieldBindings.asJava)
        .build()

    // Build an AvroScoreEnvironment record.
    val avroScoreEnvironment: AvroScoreEnvironment = AvroScoreEnvironment
        .newBuilder()
        .setKvStores(scoreEnvironment.kvstores.asJava)
        .setOutputColumn(scoreEnvironment.outputColumn)
        .build()

    // Build an AvroModelEnvironment record.
    val environment: AvroModelEnvironment = AvroModelEnvironment
        .newBuilder()
        .setName(name)
        .setVersion(version)
        .setProtocolVersion(protocolVersion.toString)
        .setModelTableUri(modelTableUri)
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
   * @param extractEnvironment defining configuration details specific to the Extract phase of
   *     a model.
   * @param scoreEnvironment defining configuration details specific to the Score phase of a
   *     model.
   */
  final def withNewSettings(
      name: String = this.name,
      version: String = this.version,
      modelTableUri: String = this.modelTableUri,
      extractEnvironment: ExtractEnvironment = this.extractEnvironment,
      scoreEnvironment: ScoreEnvironment = this.scoreEnvironment): ModelEnvironment = {
    new ModelEnvironment(
        name,
        version,
        modelTableUri,
        extractEnvironment,
        scoreEnvironment)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case environment: ModelEnvironment => {
        name == environment.name &&
            version == environment.version &&
            modelTableUri == environment.modelTableUri &&
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
      extractEnvironment: ExtractEnvironment,
      scoreEnvironment: ScoreEnvironment): ModelEnvironment = {
    new ModelEnvironment(name, version, modelTableUri, extractEnvironment, scoreEnvironment)
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

    // Load the extractor's model environment.
    val extractEnvironment = new ExtractEnvironment(
        dataRequest = avroToKijiDataRequest(
            avroModelEnvironment.getExtractEnvironment.getDataRequest),
        fieldBindings = avroModelEnvironment.getExtractEnvironment.getFieldBindings.asScala,
        kvstores = avroModelEnvironment.getExtractEnvironment.getKvStores.asScala)

    // Load the scorer's model environment.
    val scoreEnvironment = new ScoreEnvironment(
        outputColumn = avroModelEnvironment.getScoreEnvironment.getOutputColumn,
        kvstores = avroModelEnvironment.getScoreEnvironment.getKvStores.asScala)

    // Build a model environment.
    new ModelEnvironment(
        name = avroModelEnvironment.getName,
        version = avroModelEnvironment.getVersion,
        modelTableUri = avroModelEnvironment.getModelTableUri,
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
    validateDataRequest(avroDataRequest)

    val builder = KijiDataRequest.builder()
        .withTimeRange(avroDataRequest.getMinTimestamp(), avroDataRequest.getMaxTimestamp())

    // Add columns to the datarequest.
    avroDataRequest
        .getColumnDefinitions
        .asScala
        .foreach { columnSpec: ColumnSpec =>
      val name = new KijiColumnName(columnSpec.getName())
      val maxVersions = columnSpec.getMaxVersions()
      // TODO(EXP-62): Add support for filters.

      builder.newColumnsDef().withMaxVersions(maxVersions).add(name)
    }

    builder.build()
  }

  /**
   * Converts a Kiji data request to an Avro data request.
   *
   * @param kijiDataRequest to convert.
   * @return an Avro data request converted from the provided Kiji data request.
   */
  private[express] def kijiToAvroDataRequest(kijiDataRequest: KijiDataRequest): AvroDataRequest = {
    val columns: Seq[ColumnSpec] = kijiDataRequest
        .getColumns()
        .asScala
        .map { column =>
          ColumnSpec
              .newBuilder()
              .setName(column.getName())
              .setMaxVersions(column.getMaxVersions())
              // TODO(EXP-62): Add support for filters.
              .build()
        }
        .toSeq

    // Build an Avro data request.
    AvroDataRequest
        .newBuilder()
        .setMinTimestamp(kijiDataRequest.getMinTimestamp())
        .setMaxTimestamp(kijiDataRequest.getMaxTimestamp())
        .setColumnDefinitions(columns.asJava)
        .build()
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
    val extractEnvironment = environment.extractEnvironment
    val scoreEnvironment = environment.scoreEnvironment
    val kvstores = extractEnvironment.kvstores

    val fieldNames: Seq[String] = extractEnvironment.fieldBindings.map {
      fieldBinding: FieldBinding => fieldBinding.getTupleFieldName
    }
    val columnNames: Seq[String] = extractEnvironment.fieldBindings.map {
      fieldBinding: FieldBinding => fieldBinding.getStoreFieldName
    }

    // Collect errors from other validation steps.
    val validationErrors: Seq[Option[ValidationException]] = Seq(
        validateProtocolVersion(environment.protocolVersion),
        validateName(environment.name),
        validateVersion(environment.version),
        validateFieldNames(fieldNames),
        validateColumnNames(columnNames),
        validateKijiColumnName(scoreEnvironment.outputColumn),
        validateScoreOutputColumn(scoreEnvironment.outputColumn)
    )

    // Validate column names.
    val columnNameErrors: Seq[Option[ValidationException]] = columnNames.map {
      columnName: String =>
        validateKijiColumnName(columnName)
    }

    // Collect KVStore errors.
    val kvstoreNameErrors: Seq[Option[ValidationException]] = kvstores.map {
      kvstore: KVStore => validateKvstoreName(kvstore)
    }
    val kvstorePropertiesErrors: Seq[Option[ValidationException]] = kvstores.map {
      kvstore: KVStore => validateKvstoreProperties(kvstore)
    }
    val kvstoreErrors = kvstoreNameErrors ++ kvstorePropertiesErrors

    // Throw an exception if there were any validation errors.
    val allErrors = validationErrors ++ columnNameErrors ++ kvstoreErrors
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
   * Verifies that a model definition's version string is valid.
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
   * Validates a data request. Currently this only validates the column names.
   *
   * @param dataRequest to validate.
   * @throws a ModelEnvironmentValidationException if the column names are invalid.
   */
  def validateDataRequest(dataRequest: AvroDataRequest) {
    val errors = dataRequest
        .getColumnDefinitions
        .asScala
        .flatMap { column: ColumnSpec =>
          validateKijiColumnName(column.getName())
        }

    if (!errors.isEmpty) {
      throw new ModelEnvironmentValidationException(errors)
    }
  }

  /**
   * Validate the name of a KVStore.
   *
   * @param kvstore whose name should be validated.
   * @return an optional ValidationException if the name provided is invalid.
   */
  def validateKvstoreName(kvstore: KVStore): Option[ValidationException] = {
    if (!kvstore.getName.matches("^[a-zA-Z_][a-zA-Z0-9_]+$")) {
      val error = "The key-value store name must begin with a letter" +
          " and contain only alphanumeric characters and underscores. The key-value store" +
          " name you provided is " + kvstore.getName + " and the regex it must match is " +
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
    val properties: Iterable[Property] = kvstore.getProperties().asScala
    def propertiesContains(name: String): Boolean = {
      properties.count { property =>
        property.getName() == name
      } > 0
    }

    kvstore.getStoreType match {
      case KvStoreType.AVRO_KV => {
        if (!propertiesContains("path")) {
          val error = "To use an Avro key-value record key-value store, you must specify the" +
              " HDFS path to the Avro container file to use to back the store. Use the" +
              " property name 'path' to provide the path."
          return Some(new ValidationException(error))
        }
      }

      case KvStoreType.AVRO_RECORD => {
        // Construct an error message for a missing path, missing key_field, or both.
        val pathError =
          if (!propertiesContains("path")) {
            "To use an Avro record key-value store, you must specify the HDFS path" +
                " to the Avro container file to use to back the store. Use the property name" +
                " 'path' to provide the path."
          } else {
            ""
          }

        val keyFieldError =
          if (!propertiesContains("key_field")) {
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
        if (!propertiesContains("uri")) {
          val error = "To use a Kiji table key-value store, you must specify a Kiji URI" +
              " addressing the table to use to back the store. Use the property name 'url' to" +
              " provide the URI."
          return Some(new ValidationException(error))
        }
        if (!propertiesContains("column")) {
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
   * Validates a column name specified in the model environment as a fully qualified
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

    val columnName = new KijiColumnName(name)
    if (!columnName.isFullyQualified()) {
      val error = "The name " + name + " provided as the data binding must identify a" +
          " fully qualified column, not a column family."
      return Some(new ValidationException(error))
    }
    None
  }
}
