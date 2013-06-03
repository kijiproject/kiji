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

import java.io.File

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
 *         "max_versions" : 3,
 *         "filter" : null
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
  private[express] val VALIDATION_MESSAGE = "One or more errors occurred while validating your " +
      "model environment. Please correct the problems in your model environment and try again."

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
    val builder = KijiDataRequest.builder()
        .withTimeRange(avroDataRequest.getMinTimestamp(), avroDataRequest.getMaxTimestamp())

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
   * Verifies that all fields in a model environment are valid. This validation method will collect
   * all validation errors into one exception.
   *
   * @param environment to validate.
   * @throws ModelEnvironmentValidationException if there are errors encountered while validating
   *     the provided model definition.
   */
  def validateModelEnvironment(environment: ModelEnvironment) {
    val extractEnvironment = Option(environment.extractEnvironment)
    val scoreEnvironment = Option(environment.scoreEnvironment)

    // Collect errors from other validation steps.
    val errors: Seq[Option[ValidationException]] = Seq(
        catchError(validateProtocolVersion(environment.protocolVersion)),
        catchError(validateName(environment.name)),
        catchError(validateVersion(environment.version)),
        extractEnvironment
            .flatMap { x => catchError(validateExtractEnvironment(x)) },
        scoreEnvironment
            .flatMap { x => catchError(validateScoreEnvironment(x)) })

    // Throw an exception if there were any validation errors.
    val causes = errors.flatten
    if (!causes.isEmpty) {
      throw new ModelEnvironmentValidationException(causes)
    }
  }

  /**
   * Verifies that a model environment's protocol version is supported.
   *
   * @param protocolVersion to validate.
   */
  def validateProtocolVersion(protocolVersion: ProtocolVersion) {
    if (MAX_RUN_ENV_VER.compareTo(protocolVersion) < 0) {
      val error = "\"%s\" is the maximum protocol version supported. ".format(MAX_RUN_ENV_VER) +
          "The provided model environment is of protocol version: \"%s\"".format(protocolVersion)

      throw new ValidationException(error)
    } else if (MIN_RUN_ENV_VER.compareTo(protocolVersion) > 0) {
      val error = "\"%s\" is the minimum protocol version supported. ".format(MIN_RUN_ENV_VER) +
          "The provided model environment is of protocol version: \"%s\"".format(protocolVersion)

      throw new ValidationException(error)
    }
  }

  /**
   * Verifies that a model environment's name is valid.
   *
   * @param name to validate.
   */
  def validateName(name: String) {
    if(name.isEmpty) {
      throw new ValidationException("The name of the model environment can not be the empty " +
          "string.")
    } else if (!KijiNameValidator.isValidAlias(name)) {
      throw new ValidationException("The name \"%s\" is not valid. ".format(name) +
          "Names must match the regex \"%s\"."
              .format(KijiNameValidator.VALID_ALIAS_PATTERN.pattern))
    }
  }

  /**
   * Verifies that a model definition's version string is valid.
   *
   * @param version to validate.
   */
  def validateVersion(version: String) {
    if (!version.matches(VERSION_REGEX)) {
      val error = "Model environment version strings must match the regex " +
          "\"%s\" (1.0.0 would be valid).".format(VERSION_REGEX)
      throw new ValidationException(error)
    }
  }

  /**
   * Validates fields in the extract model environment.
   *
   * @param extractEnvironment to validate.
   */
  def validateExtractEnvironment(extractEnvironment: ExtractEnvironment) {
    val kvstores = Option(extractEnvironment.kvstores)

    // Store errors that occur during other validation steps.
    val errors = Seq(
        catchError(validateExtractBindings(extractEnvironment.fieldBindings)),
        kvstores
            .flatMap { x => catchError(validateKvstores(x)) })

    val causes = errors.flatten
    if (!causes.isEmpty) {
      throw new ModelEnvironmentValidationException(causes)
    }
  }

  /**
   * Validates fields in the score model environment.
   *
   * @param scoreEnvironment to validate.
   */
  def validateScoreEnvironment(scoreEnvironment: ScoreEnvironment) {
    val kvstores = Option(scoreEnvironment.kvstores)

    // Store errors that occur during other validation steps.
    val errors = Seq(
        catchError(validateScoreBinding(scoreEnvironment.outputColumn)),
        kvstores
            .flatMap { x => catchError(validateKvstores(x)) })

    val causes = errors.flatten
    if (!causes.isEmpty) {
      throw new ModelEnvironmentValidationException(causes)
    }
  }

  /**
   * Validates a data request. Currently this only validates the format of the column name.
   *
   * @param dataRequest to validate.
   */
  def validateDataRequest(dataRequest: AvroDataRequest) {
    val errors = dataRequest
        .getColumnDefinitions
        .asScala
        .flatMap { column: ColumnSpec =>
          if (!column.getName.matches("^[a-zA-Z_][a-zA-Z0-9_:]+$")) {
            val error = "Columns names must begin with a letter and contain only alphanumeric " +
                "characters and underscores. The column name you provided " +
                "(%s) must match the regular expression ".format(column.getName) +
                " /^[a-zA-Z_][a-zA-Z0-9_:]+$/ ."
            Some(new ValidationException(error))
          } else {
            None
          }
        }

    if (!errors.isEmpty) {
      throw new ModelEnvironmentValidationException(errors)
    }
  }

  /**
   * Validates specified keyvalue stores in the model environment. Currently, this only validates
   * the format of the name.
   *
   * @param kvstores to validate.
   */
  def validateKvstores(kvstores: Seq[KVStore]) {
    kvstores.foreach { kvStore: KVStore =>
      if (!kvStore.getName.matches("^[a-zA-Z_][a-zA-Z0-9_]+$")) {
        throw new ValidationException("The kvstore name must begin with a letter" +
            " and contain only alphanumeric characters and underscores. The kvstore name you" +
            " provided is " + kvStore.getName + " and the regex it must match is" +
            " ^[a-zA-Z_][a-zA-Z0-9_]+$" )
      }
    }
  }

  /**
   * Validates the tuple field names to data bindings in the extract phase. This checks that every
   * field name is uniquely mapped to a column name and vice versa. This also verifies that the
   * supplied column names are formatted correctly.
   *
   * @param bindings to validate.
   */
  def validateExtractBindings(bindings: Seq[FieldBinding]) {
    val fieldNames: Seq[String] = bindings.map { fieldBinding: FieldBinding =>
      fieldBinding.getTupleFieldName
    }
    if (fieldNames.distinct.size != fieldNames.size) {
      throw new ValidationException("Every tuple field name must map to a" +
          " unique column name. You have a field name that is associated with multiple columns.")
    }
    val columnNames: Seq[String] = bindings.map { fieldBinding: FieldBinding =>
      fieldBinding.getStoreFieldName
    }
    if (columnNames.distinct.size != columnNames.size) {
      throw new ValidationException("Every column name must map to a unique tuple" +
          " field. You have a data field associated with multiple columns. ")
    }
    columnNames.foreach { columnName: String =>
      if (!columnName.matches("^[a-zA-Z_][a-zA-Z0-9_:]+$")) {
        throw new ValidationException("The column name provided as the data binding" +
            " in the extract phase must begin with a letter and only consist of alphanumeric" +
            " characters and underscores. The column name you provided is: " + columnName + " and" +
            " the regex it must match is: ^[a-zA-Z_][a-zA-Z0-9_:]+$")
      }
      if (columnName.isEmpty) {
        throw new ValidationException("Column names can not be the empty string.")
      }
    }
  }

  /**
   * Validates the specified output column for the score phase.
   *
   * @param outputColumn to validate.
   */
  def validateScoreBinding(outputColumn: String) {
    if (outputColumn.isEmpty) {
      throw new ValidationException("The column to write out to in the score phase" +
          "can not be the empty string.")
    }
    val columnName: KijiColumnName = try {
      new KijiColumnName(outputColumn)
    } catch {
      case e: IllegalArgumentException => {
        throw new ValidationException("Invalid output column: " + e.getMessage())
      }
    }
    if (!columnName.isFullyQualified()) {
      throw new ValidationException("The name provided as the data binding must identify a fully " +
          "qualified column, not a column family.")
    }
  }

  def validateColumnName(name: String) {
  }
}
