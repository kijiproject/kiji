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

package org.kiji.modeling.impl

import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Source
import com.twitter.scalding.TextLine
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiOutput
import org.kiji.mapreduce.KijiContext
import org.kiji.mapreduce.kvstore.lib.{ AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroRecordKeyValueStore => JAvroRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ KijiTableKeyValueStore => JKijiTableKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ TextFileKeyValueStore => JTextFileKeyValueStore }
import org.kiji.mapreduce.kvstore.{ KeyValueStore => JKeyValueStore }
import org.kiji.mapreduce.kvstore.{ KeyValueStoreReader => JKeyValueStoreReader }
import org.kiji.modeling.KeyValueStore
import org.kiji.modeling.config.InputSpec
import org.kiji.modeling.config.KeyValueStoreSpec
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiOutputSpec
import org.kiji.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.OutputSpec
import org.kiji.modeling.config.SequenceFileSourceSpec
import org.kiji.modeling.config.TextSourceSpec
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowKeyComponents
import org.kiji.schema.KijiURI

/**
 * Utility object for the model lifecycle. Transforms the various input, output and key-value
 * specifications from the [[org.kiji.modeling.config.ModelEnvironment]] to classes used
 * in the model lifecycle.
 */
object ModelJobUtils {

  /**
   * Trait to describe the phase of the model lifecycle.
   */
  sealed trait PhaseType

  /**
   * Companion object for [[org.kiji.modeling.impl.ModelJobUtils.PhaseType]].
   */
  object PhaseType {
    object PREPARE extends PhaseType
    object TRAIN extends PhaseType
    object SCORE extends PhaseType
  }

  /**
   * Returns the name of the Kiji column this phase will write to.
   *
   * This method reads the Score phase's output column from this model's run profile and returns it.
   *
   * @param modelEnvironment from which to retrieve the output column for the score environment.
   * @return the output column name.
   */
  def getOutputColumn(modelEnvironment: ModelEnvironment): String = modelEnvironment
      .scoreEnvironment
      .get
      .outputSpec
      .asInstanceOf[KijiSingleColumnOutputSpec]
      .outputColumn
      .columnName
      .toString

  /**
   * Wrap the provided key value stores in their scala counterparts.
   *
   * @param keyValueStoreSpecs to open.
   * @param context providing access to the opened key value stores.
   * @return a mapping from the keyValueStoreSpec's name to the wrapped keyValueStoreSpec.
   */
  def wrapKvstoreReaders(
      keyValueStoreSpecs: Seq[KeyValueStoreSpec],
      context: KijiContext): Map[String, KeyValueStore[_, _]] = {
    keyValueStoreSpecs
        .map { keyValueStoreSpec: KeyValueStoreSpec =>
          val jKeyValueStoreReader = context.getStore(keyValueStoreSpec.name)
          val wrapped: KeyValueStore[_, _] = keyValueStoreSpec.storeType match {
            case "KIJI_TABLE" => { // special conversions are needed here
              KeyValueStore.kijiTableKeyValueStore(jKeyValueStoreReader
                  .asInstanceOf[JKeyValueStoreReader[KijiRowKeyComponents, _]])
            }
            case _ => KeyValueStore(jKeyValueStoreReader.asInstanceOf[JKeyValueStoreReader[_, _]])
          }
          (keyValueStoreSpec.name, wrapped)
        }
        .toMap
  }

  /**
   * Open the provided key value store specifications.
   *
   * @param keyValueStoreSpecs to open.
   * @param configuration containing settings pertaining to the specified key value stores.
   * @return a mapping from the key value store specification's name to the opened key value store.
   */
  def openJKvstores(
      keyValueStoreSpecs: Seq[KeyValueStoreSpec],
      configuration: Configuration): Map[String, JKeyValueStore[_, _]] = {
    keyValueStoreSpecs
        // Open the key value stores defined for the extract phase.
        .map { keyValueStoreSpec: KeyValueStoreSpec =>
          val properties = keyValueStoreSpec.properties

          // Handle each type of keyValueStoreSpec differently.
          val jKeyValueStore: JKeyValueStore[_, _] = keyValueStoreSpec.storeType match {
            case "AVRO_KV" => {
              // Open AvroKV.
              val builder = JAvroKVRecordKeyValueStore
                  .builder()
                  .withConfiguration(configuration)
                  .withInputPath(new Path(properties("path")))
              if (properties.contains("use_dcache")) {
                builder
                    .withDistributedCache(properties("use_dcache") == "true")
                    .build()
              } else {
                builder.build()
              }
            }
            case "AVRO_RECORD" => {
              // Open AvroRecord.
              val builder = JAvroRecordKeyValueStore
                  .builder()
                  .withConfiguration(configuration)
                  .withKeyFieldName(properties("key_field"))
                  .withInputPath(new Path(properties("path")))
              if (properties.contains("use_dcache")) {
                builder
                    .withDistributedCache(properties("use_dcache") == "true")
                    .build()
              } else {
                builder.build()
              }
            }
            case "KIJI_TABLE" => {
              // Kiji table.
              val uri: KijiURI = KijiURI.newBuilder(properties("uri")).build()
              val columnName: KijiColumnName = new KijiColumnName(properties("column"))
              JKijiTableKeyValueStore
                  .builder()
                  .withConfiguration(configuration)
                  .withTable(uri)
                  .withColumn(columnName.getFamily, columnName.getQualifier)
                  .build()
            }
            case "TEXT_FILE" => {
              val builder = JTextFileKeyValueStore
                  .builder()
                  .withConfiguration(configuration)
                  .withDelimiter(properties("delimiter"))
                  .withInputPath(new Path(properties("path")))
              if (properties.contains("use_dcache")) {
                builder
                    .withDistributedCache(properties("use_dcache") == "true")
                    .build()
              } else {
                builder.build()
              }
            }
            case keyValueStoreType => throw new UnsupportedOperationException(
                "KeyValueStores of type \"%s\" are not supported"
                    .format(keyValueStoreType.toString))
          }

          // Pack the keyValueStoreSpec into a tuple with its name.
          (keyValueStoreSpec.name, jKeyValueStore)
        }
        .toMap
  }

  /**
   * Convert a map of input specifications to the corresponding Scalding sources.
   *
   * @param modelEnvironment for the conversion.
   * @param phase for the conversion.
   * @return a map from the source name to the Scalding source.
   */
  def inputSpecsToSource(
      modelEnvironment: ModelEnvironment,
      phase: PhaseType
  ): Map[String, Source] = {
    phase match {
      case PhaseType.PREPARE => {
        val prepareEnv = modelEnvironment
            .prepareEnvironment
            .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
        prepareEnv
            .inputSpec
            .map { entry: (String, InputSpec) =>
              val (inputName, spec) = entry
              val inputSource = inputSpecToSource(spec)

              (inputName, inputSource)
            }
      }
      case PhaseType.TRAIN => {
        val trainEnv = modelEnvironment
            .trainEnvironment
            .getOrElse { throw new IllegalArgumentException("Train environment does not exist") }
        trainEnv
            .inputSpec
            .map { entry: (String, InputSpec) =>
              val (inputName, spec) = entry
              val inputSource = inputSpecToSource(spec)

              (inputName, inputSource)
            }
      }
      case _ => throw new IllegalArgumentException("Invalid phase type. Multiple specifications " +
        "can only be used with a prepare and train phase. Did you mean to call inputSpecToSource?")
    }
  }

  /**
   * Convert a map of output specifications to the corresponding Scalding sources.
   * @param modelEnvironment for the conversion.
   * @param phase for the conversion.
   * @return a map from the source name to the Scalding source.
   */
  def outputSpecsToSource(
      modelEnvironment: ModelEnvironment,
      phase: PhaseType
  ): Map[String, Source] = {
    phase match {
      case PhaseType.PREPARE => {
        val prepareEnv = modelEnvironment
            .prepareEnvironment
            .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
        prepareEnv
            .outputSpec
            .map { entry: (String, OutputSpec) =>
              val (outputName, spec) = entry
              val outputSource = outputSpecToSource(modelEnvironment, PhaseType.PREPARE, spec)

              (outputName, outputSource)
            }
      }
      case PhaseType.TRAIN => {
        val trainEnv = modelEnvironment
            .trainEnvironment
            .getOrElse { throw new IllegalArgumentException("Train environment does not exist") }
        trainEnv
            .outputSpec
            .map { entry: (String, OutputSpec) =>
              val (outputName, spec) = entry
              val outputSource = outputSpecToSource(modelEnvironment, PhaseType.TRAIN, spec)

              (outputName, outputSource)
            }
      }
      case _ => throw new IllegalArgumentException("Invalid phase type. Multiple specifications " +
        "can only be used with a prepare and train phase. Did you mean to call inputSpecToSource?")
    }
  }

  /**
   * Convert an output specification from a [[org.kiji.modeling.config.ModelEnvironment]]
   * into a Scalding [[com.twitter.scalding.Source]] that can be used by the phases of the model
   * lifecycle.
   *
   * @param modelEnvironment from which to retrieve the Source.
   * @param phase for which to create a Source.
   * @param outputSpec with which to construct the source. Used for the prepare and train phase.
   * @return the output [[com.twitter.scalding.Source]] created for the given phase.
   */
  def outputSpecToSource(
      modelEnvironment: ModelEnvironment,
      phase: PhaseType,
      outputSpec: OutputSpec): Source = {
    outputSpec match {
      case spec: KijiOutputSpec => {
        spec.timestampField match {
          case Some(field) => KijiOutput(spec.tableUri, field, spec.fieldsToColumns)
          case None => KijiOutput(spec.tableUri, spec.fieldsToColumns)
        }
      }

      case TextSourceSpec(path) => {
        TextLine(path)
      }
      case SequenceFileSourceSpec(path, Some(keyField), Some(valueField)) => {
        val fields = new Fields(keyField, valueField)
        SequenceFile(path, fields)
      }
      case SequenceFileSourceSpec(path, None, None) => {
        SequenceFile(path)
      }
      case _ => throw new IllegalArgumentException("Prepare environment does not exist")
    }
  }

  /**
   * Create a KijiDataRequest from a model environment.
   *
   * @param modelEnvironment from which to generate the data request.
   * @param phase in the model lifecycle in which to find the input spec corresponding to the data
   *     request.
   * @param inputSpecName The name of the input spec corresponding to the data request.  Not
   *     necessary if `phase` is `PhaseType.SCORE.`
   * @return An `Option[KijiDataRequest]` that will request the data described in the input spec
   *     indicated by `modelEnvironment`, `phase`, and `inputSpecName`.
   */
  def getDataRequest(
      modelEnvironment: ModelEnvironment,
      phase: PhaseType,
      inputSpecName: String = ""): Option[KijiDataRequest] = {

    val inputSpec: Option[InputSpec] = phase match {
      case PhaseType.PREPARE => modelEnvironment.prepareEnvironment.map { environment =>
          environment.inputSpec.getOrElse(inputSpecName, null)}
      case PhaseType.TRAIN => modelEnvironment.trainEnvironment.map { environment =>
          environment.inputSpec.getOrElse(inputSpecName, null) }
      case PhaseType.SCORE => modelEnvironment.scoreEnvironment.map { _.inputSpec }
    }

    inputSpec
        .map {
          case x: KijiInputSpec => x.toKijiDataRequest
          case _ => throw new RuntimeException("Input Specification is not of type KijiInputSpec")
        }
  }

  /**
   * Get a Scalding Source from an InputSpec.
   *
   * @param inputSpec containing the specification for the Scaling source.
   * @return a Scalding source.
   */
  def inputSpecToSource(inputSpec: InputSpec): Source = {
    inputSpec match {
      // After refactoring for EXP-232, KijiInputSpec members match up perfectly with KijiInput
      case spec @ KijiInputSpec(tableUri, timeRange, columns) => {
        KijiInput(tableUri, timeRange, columns)
      }
      case spec @ TextSourceSpec(path) => {
        TextLine(path)
      }
      case SequenceFileSourceSpec(path, Some(keyField), Some(valueField)) => {
        val fields = new Fields(keyField, valueField)
        SequenceFile(path, fields)
      }
      case SequenceFileSourceSpec(path, None, None) => {
        SequenceFile(path)
      }
      case _ => throw new IllegalArgumentException("Prepare environment does not exist")
    }
  }

}
