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
import org.kiji.express.Resources.doAndClose
import org.kiji.express.avro.AvroDataRequest
import org.kiji.express.avro.AvroExtractRunSpec
import org.kiji.express.avro.AvroRunSpec
import org.kiji.express.avro.AvroScoreRunSpec
import org.kiji.express.avro.ColumnSpec
import org.kiji.express.avro.KVStore
import org.kiji.express.avro.FieldBinding
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.KijiNameValidator
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson

/**
 * A specification of the runtime bindings needed in the extract phase of a model pipeline.
 *
 * @param dataRequest describing the input columns required during the extract phase.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how data that is requested for the extract phase is mapped onto named
 *     input fields.
 * @param kvstores for usage during the extract phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ExtractRunSpec private[express] (
    val dataRequest: KijiDataRequest,
    val fieldBindings: Seq[FieldBinding],
    val kvstores: Seq[KVStore]) {
  override def equals(other: Any): Boolean = {
    other match {
      case spec: ExtractRunSpec => {
        dataRequest == spec.dataRequest &&
            fieldBindings == spec.fieldBindings &&
            kvstores == spec.kvstores
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
 * A specification of the runtime bindings for data sources required in the score phase of a model
 * pipeline.
 *
 * @param outputColumn to write scores to.
 * @param kvstores for usage during the score phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ScoreRunSpec private[express] (
    val outputColumn: String,
    val kvstores: Seq[KVStore]) {
  override def equals(other: Any): Boolean = {
    other match {
      case spec: ScoreRunSpec => {
        outputColumn == spec.outputColumn &&
            kvstores == spec.kvstores
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
 * A RunSpec is a specification describing how to execute a linked model specification. This
 * includes specifying:
 * <ul>
 *   <li>A model specification to run</li>
 *   <li>Mappings from logical names of data sources to their physical realizations</li>
 * </ul>
 *
 * Currently this class can only be created from a run specification in a JSON file. JSON run
 * specifications should be written using the following format:
 * {{{
 * {
 *   "name" : "myRunProfile",
 *   "version" : "1.0.0",
 *   "model_table_uri" : "kiji://.env/default/mytable",
 *   "protocol_version" : "run_spec-0.1.0",
 *   "extract_run_spec" : {
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
 *       "field_name" : "in",
 *       "column_name" : "info:in"
 *     } ]
 *   },
 *   "score_run_spec" : {
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
 * To load a JSON run specification:
 * {{{
 * // Load a JSON string directly:
 * val myRunSpec: RunSpec = RunSpec.loadJson("""{ "name": "myIdentifier", ... }""")
 *
 * // Load a JSON file:
 * val myRunSpec2: RunSpec = RunSpec.loadJsonFile("/path/to/json/config.json")
 * }}}
 *
 * @param name of the run specification.
 * @param version of the run specification.
 * @param modelTableUri is the URI of the Kiji table this run specification will read from and
 *     write to.
 * @param extractRunSpec defining configuration details specific to the Extract phase of a model.
 * @param scoreRunSpec defining configuration details specific to the Score phase of a model.
 * @param protocolVersion this model specification was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class RunSpec private[express] (
    val name: String,
    val version: String,
    val modelTableUri: String,
    val extractRunSpec: ExtractRunSpec,
    val scoreRunSpec: ScoreRunSpec,
    private[express] val protocolVersion: ProtocolVersion = RunSpec.CURRENT_MODEL_SPEC_VER) {
  // Ensure that all fields set for this run spec are valid.
  RunSpec.validateRunSpec(this)

  /**
   * Serializes this run specification into a JSON string.
   *
   * @return a JSON string that represents this run specification.
   */
  final def toJson(): String = {
    // Build an AvroExtractRunSpec record.
    val extractSpec: AvroExtractRunSpec = AvroExtractRunSpec
        .newBuilder()
        .setDataRequest(RunSpec.kijiToAvroDataRequest(extractRunSpec.dataRequest))
        .setKvStores(extractRunSpec.kvstores.asJava)
        .setFieldBindings(extractRunSpec.fieldBindings.asJava)
        .build()

    // Build an AvroScoreRunSpec record.
    val scoreSpec: AvroScoreRunSpec = AvroScoreRunSpec
        .newBuilder()
        .setKvStores(scoreRunSpec.kvstores.asJava)
        .setOutputColumn(scoreRunSpec.outputColumn)
        .build()

    // Build an AvroRunSpec record.
    val spec: AvroRunSpec = AvroRunSpec
        .newBuilder()
        .setName(name)
        .setVersion(version)
        .setProtocolVersion(protocolVersion.toString)
        .setModelTableUri(modelTableUri)
        .setExtractRunSpec(extractSpec)
        .setScoreRunSpec(scoreSpec)
        .build()

    ToJson.toAvroJsonString(spec)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case spec: RunSpec => {
        name == spec.name &&
            version == spec.version &&
            modelTableUri == spec.modelTableUri &&
            extractRunSpec == spec.extractRunSpec &&
            scoreRunSpec == spec.scoreRunSpec &&
            protocolVersion == spec.protocolVersion
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(
          name,
          version,
          modelTableUri,
          extractRunSpec,
          scoreRunSpec,
          protocolVersion)
}

/**
 * Companion object for RunSpec. Contains constants related to run specifications as well as
 * validation methods.
 */
object RunSpec {
  /** Maximum run specification version we can recognize. */
  val MAX_RUN_SPEC_VER: ProtocolVersion = ProtocolVersion.parse("run_spec-0.1.0")

  /** Minimum run specification version we can recognize. */
  val MIN_RUN_SPEC_VER: ProtocolVersion = ProtocolVersion.parse("run_spec-0.1.0")

  /** Current ModelSpec protocol version.  */
  val CURRENT_MODEL_SPEC_VER: ProtocolVersion = ProtocolVersion.parse("run_spec-0.1.0")

  /** Regular expression used to validate a run spec version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model specification. */
  private[express] val VALIDATION_MESSAGE = "One or more errors occurred while validating your " +
      "run specification. Please correct the problems in your run specification and try again."

  /**
   * Creates a RunSpec given a JSON string. In the process, all fields are validated.
   *
   * @param json serialized run specification.
   * @return the validated run specification.
   */
  def fromJson(json: String): RunSpec = {
    // Parse the json.
    val avroRunSpec: AvroRunSpec = FromJson
        .fromJsonString(json, AvroRunSpec.SCHEMA$)
        .asInstanceOf[AvroRunSpec]
    val protocol = ProtocolVersion
        .parse(avroRunSpec.getProtocolVersion)

    // Load the extractor's run specification.
    val extractSpec = new ExtractRunSpec(
        dataRequest = avroToKijiDataRequest(avroRunSpec.getExtractRunSpec.getDataRequest),
        fieldBindings = avroRunSpec.getExtractRunSpec.getFieldBindings.asScala,
        kvstores = avroRunSpec.getExtractRunSpec.getKvStores.asScala)

    // Load the scorer's run specification.
    val scoreSpec = new ScoreRunSpec(
        outputColumn = avroRunSpec.getScoreRunSpec.getOutputColumn,
        kvstores = avroRunSpec.getScoreRunSpec.getKvStores.asScala)

    // Build a run specification.
    new RunSpec(
        name = avroRunSpec.getName,
        version = avroRunSpec.getVersion,
        modelTableUri = avroRunSpec.getModelTableUri,
        extractRunSpec = extractSpec,
        scoreRunSpec = scoreSpec,
        protocolVersion = protocol)
  }

  /**
   * Creates an RunSpec given a path in the local filesystem to a JSON file that specifies a run
   * specification. In the process, all fields are validated.
   *
   * @param path in the local filesystem to a JSON file containing a run specification.
   * @return the validated run specification.
   */
  def fromJsonFile(path: String): RunSpec = {
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
   * Verifies that all fields in a run specification are valid. This validation method will collect
   * all validation errors into one exception.
   *
   * @param spec to validate.
   * @throws RunSpecValidationException if there are errors encountered while validating the
   *     provided model spec.
   */
  def validateRunSpec(spec: RunSpec) {
    val extractorSpec = Option(spec.extractRunSpec)
    val scoreSpec = Option(spec.scoreRunSpec)

    // Collect errors from other validation steps.
    val errors: Seq[Option[ValidationException]] = Seq(
        catchError(validateProtocolVersion(spec.protocolVersion)),
        catchError(validateName(spec.name)),
        catchError(validateVersion(spec.version)),
        extractorSpec
            .flatMap { x => catchError(validateExtractSpec(x)) },
        scoreSpec
            .flatMap { x => catchError(validateScoreSpec(x)) })

    // Throw an exception if there were any validation errors.
    val causes = errors.flatten
    if (!causes.isEmpty) {
      throw new RunSpecValidationException(causes)
    }
  }

  /**
   * Verifies that a run specification's protocol version is supported.
   *
   * @param protocolVersion to validate.
   */
  def validateProtocolVersion(protocolVersion: ProtocolVersion) {
    if (MAX_RUN_SPEC_VER.compareTo(protocolVersion) < 0) {
      val error = "\"%s\" is the maximum protocol version supported. ".format(MAX_RUN_SPEC_VER) +
          "The provided run spec is of protocol version: \"%s\"".format(protocolVersion)

      throw new ValidationException(error)
    } else if (MIN_RUN_SPEC_VER.compareTo(protocolVersion) > 0) {
      val error = "\"%s\" is the minimum protocol version supported. ".format(MIN_RUN_SPEC_VER) +
          "The provided run spec is of protocol version: \"%s\"".format(protocolVersion)

      throw new ValidationException(error)
    }
  }

  /**
   * Verifies that a run specification's name is valid.
   *
   * @param name to validate.
   */
  def validateName(name: String) {
    if(name.isEmpty) {
      throw new ValidationException("The name of the run spec can not be the empty string.")
    } else if (!KijiNameValidator.isValidAlias(name)) {
      throw new ValidationException("The name \"%s\" is not valid. ".format(name) +
          "Names must match the regex \"%s\"."
              .format(KijiNameValidator.VALID_ALIAS_PATTERN.pattern))
    }
  }

  /**
   * Verifies that a model specification's version string is valid.
   *
   * @param version to validate.
   */
  def validateVersion(version: String) {
    if (!version.matches(VERSION_REGEX)) {
      val error = "Run spec version strings must match the regex \"%s\" (1.0.0 would be valid)."
          .format(VERSION_REGEX)
      throw new ValidationException(error)
    }
  }

  /**
   * Validates fields in the extract run spec.
   *
   * @param extractSpec to validate.
   */
  def validateExtractSpec(extractSpec: ExtractRunSpec) {
    val kvstores = Option(extractSpec.kvstores)

    // Store errors that occur during other validation steps.
    val errors = Seq(
        catchError(validateExtractBindings(extractSpec.fieldBindings)),
        kvstores
            .flatMap { x => catchError(validateKvstores(x)) })

    val causes = errors.flatten
    if (!causes.isEmpty) {
      throw new RunSpecValidationException(causes)
    }
  }

  /**
   * Validates fields in the score run spec.
   *
   * @param scoreSpec to validate.
   */
  def validateScoreSpec(scoreSpec: ScoreRunSpec) {
    val kvstores = Option(scoreSpec.kvstores)

    // Store errors that occur during other validation steps.
    val errors = Seq(
        catchError(validateScoreBinding(scoreSpec.outputColumn)),
        kvstores
            .flatMap { x => catchError(validateKvstores(x)) })

    val causes = errors.flatten
    if (!causes.isEmpty) {
      throw new RunSpecValidationException(causes)
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
      throw new RunSpecValidationException(errors)
    }
  }

  /**
   * Validates specified keyvalue stores in the run spec. Currently, this only validates the
   * format of the name.
   *
   * @param kvStores to validate.
   */
  def validateKvstores(kvStores: Seq[KVStore]) {
    kvStores.foreach { kvStore: KVStore =>
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
