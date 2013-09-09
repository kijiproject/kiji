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

package org.kiji.express.modeling.impl

import com.twitter.scalding.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.kiji.express.flow.Between
import org.kiji.express.flow.ColumnFamily
import org.kiji.express.flow.ColumnRequest
import org.kiji.express.flow.ColumnRequestOptions
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiOutput
import org.kiji.express.flow.QualifiedColumn
import org.kiji.express.flow.TimeRange
import org.kiji.express.modeling.KeyValueStore
import org.kiji.express.modeling.config.ExpressColumnRequest
import org.kiji.express.modeling.config.InputSpec
import org.kiji.express.modeling.config.KeyValueStoreSpec
import org.kiji.express.modeling.config.KijiInputSpec
import org.kiji.express.modeling.config.KijiOutputSpec
import org.kiji.express.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.config.OutputSpec
import org.kiji.mapreduce.KijiContext
import org.kiji.mapreduce.kvstore.{ KeyValueStore => JKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroRecordKeyValueStore => JAvroRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ KijiTableKeyValueStore => JKijiTableKeyValueStore }
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI

/**
 * Utility object for the model lifecycle. Transforms the various input, output and key-value
 * specifications from the [[org.kiji.express.modeling.config.ModelEnvironment]] to classes used
 * in the model lifecycle.
 */
object ModelJobUtils {

  /**
   * Trait to describe the phase of the model lifecycle.
   */
  sealed trait PhaseType

  /**
   * Companion object for [[org.kiji.express.modeling.impl.ModelJobUtils.PhaseType]].
   */
  object PhaseType {
    object PREPARE extends PhaseType
    object TRAIN extends PhaseType
    object SCORE extends PhaseType
  }

  /**
   * Returns a KijiDataRequest that describes which input columns need to be available to the
   * producer.
   *
   * This method reads the Extract phase's data request configuration from this model's run profile
   * and builds a KijiDataRequest from it.
   *
   * @param modelEnvironment from which to retrieve the data request.
   * @param phase for which to retrieve the data request.
   * @return a kiji data request if the phase exists or None.
   */
  def getDataRequest(
      modelEnvironment: ModelEnvironment,
      phase: PhaseType): Option[KijiDataRequest] = {
    val inputSpec: Option[InputSpec] = phase match {
      case PhaseType.PREPARE => modelEnvironment.prepareEnvironment.map { _.inputSpec }
      case PhaseType.TRAIN => modelEnvironment.trainEnvironment.map { _.inputSpec }
      case PhaseType.SCORE => modelEnvironment.scoreEnvironment.map { _.inputSpec }
    }

    inputSpec
        .map {
          case KijiInputSpec(_, dataRequest, _) => dataRequest.toKijiDataRequest
          case _ => throw new RuntimeException("Input Specification is not of type KijiInputSpec")
        }
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
            case "AVRO_KV" => new AvroKVRecordKeyValueStore(jKeyValueStoreReader)
            case "AVRO_RECORD" => new AvroRecordKeyValueStore(jKeyValueStoreReader)
            case "KIJI_TABLE" => new KijiTableKeyValueStore(jKeyValueStoreReader)
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
   * Get the [[org.kiji.express.flow.TimeRange]] for the given input specification to the model
   * environment.
   *
   * @param inputSpec of a phase in the [[org.kiji.express.modeling.config.ModelEnvironment]].
   * @return a [[org.kiji.express.flow.TimeRange]] instance for the data request.
   */
  private def getTimeRange(inputSpec: InputSpec): TimeRange = {
    inputSpec match {
      case kijiInputSpec: KijiInputSpec =>
        Between(
            kijiInputSpec.dataRequest.minTimestamp,
            kijiInputSpec.dataRequest.maxTimestamp)
      case _ => throw new IllegalStateException("Unsupported Input Specification")
    }
  }

  /**
   * Get the map from input columns to field names from an input specification.
   *
   * @param inputSpec of a phase in the [[org.kiji.express.modeling.config.ModelEnvironment]].
   * @return a map from the column requests to field names.
   */
  private def getInputColumnMap(inputSpec: KijiInputSpec): Map[ColumnRequest, Symbol] = {
    val columnMap: Map[ColumnRequest, String] = inputSpec
        .dataRequest
        .columnRequests
        .map { expressColumnRequest: ExpressColumnRequest =>
          val options = new ColumnRequestOptions(
              expressColumnRequest.maxVersions,
              expressColumnRequest.filter.map { _.toKijiColumnFilter })
          val kijiColumnName = new KijiColumnName(expressColumnRequest.name)
          val columnRequest: ColumnRequest =
              if (kijiColumnName.isFullyQualified) {
                QualifiedColumn(
                    kijiColumnName.getFamily,
                    kijiColumnName.getQualifier,
                    options)
              } else {
                // TODO specify regex matching for qualifier
                ColumnFamily(
                    kijiColumnName.getFamily,
                    None,
                    options)
              }

          columnRequest -> expressColumnRequest.name
        }
        .toMap
    val bindingMap: Map[String, Symbol] = inputSpec
        .fieldBindings
        .map { fieldBinding => fieldBinding.storeFieldName -> Symbol(fieldBinding.tupleFieldName) }
        .toMap
    columnMap.mapValues { columnName: String => bindingMap(columnName) }
  }

  /**
   * Convert an input specification from a [[org.kiji.express.modeling.config.ModelEnvironment]]
   * into a Scalding [[com.twitter.scalding.Source]] that can be used by the phases of the model
   * lifecycle.
   *
   * @param modelEnvironment from which to retrieve the Source.
   * @param phase for which to create a Source.
   * @return the input [[com.twitter.scalding.Source]] created for the given phase.
   */
  def inputSpecToSource(modelEnvironment: ModelEnvironment, phase: PhaseType): Source = {
    val inputSpec: InputSpec = phase match {
      case PhaseType.PREPARE => modelEnvironment
          .prepareEnvironment
          .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
          .inputSpec
      case PhaseType.TRAIN => modelEnvironment
          .trainEnvironment
          .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
          .inputSpec
      case PhaseType.SCORE => modelEnvironment
          .scoreEnvironment
          .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
          .inputSpec
    }
    inputSpec match {
      case spec @ KijiInputSpec(tableUri, _, _) => {
        KijiInput(tableUri, getTimeRange(spec))(getInputColumnMap(spec))
      }
      case _ => throw new IllegalArgumentException("Prepare environment does not exist")
    }
  }

  /**
   * Get a map from field names to output columns for a given output specification for a phase of
   * the model lifecycle.
   *
   * @param kijiOutputSpec is the [[org.kiji.express.modeling.config.KijiOutputSpec]] for the phase.
   * @return a map from field name to string specifying the Kiji column.
   */
  private def getOutputColumnMap(kijiOutputSpec: KijiOutputSpec): Seq[(Symbol, String)] = {
    kijiOutputSpec
        .fieldBindings
        .map { fieldBinding => Symbol(fieldBinding.tupleFieldName) -> fieldBinding.storeFieldName }
  }

  /**
   * Convert an output specification from a [[org.kiji.express.modeling.config.ModelEnvironment]]
   * into a Scalding [[com.twitter.scalding.Source]] that can be used by the phases of the model
   * lifecycle.
   *
   * @param modelEnvironment from which to retrieve the Source.
   * @param phase for which to create a Source.
   * @return the output [[com.twitter.scalding.Source]] created for the given phase.
   */
  def outputSpecToSource(modelEnvironment: ModelEnvironment, phase: PhaseType): Source = {
    val outputConfig: OutputSpec = phase match {
      case PhaseType.PREPARE => modelEnvironment
          .prepareEnvironment
          .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
          .outputSpec
      case PhaseType.TRAIN => modelEnvironment
          .trainEnvironment
          .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
          .outputSpec
      case PhaseType.SCORE => modelEnvironment
          .scoreEnvironment
          .getOrElse { throw new IllegalArgumentException("Prepare environment does not exist") }
          .outputSpec
    }
    outputConfig match {
      case spec @ KijiOutputSpec(tableUri, fieldBindings, timestampField) => {
        val outputColumnMapping: Seq[(Symbol, String)] = getOutputColumnMap(spec)
        val timestampSymbol: Symbol = timestampField
            .map { field: String => Symbol(field) }
            // scalastyle:off null
            .getOrElse { null }
            // scalastyle:on null

        KijiOutput(tableUri, timestampSymbol)(outputColumnMapping: _*)
      }
      case _ => throw new IllegalArgumentException("Prepare environment does not exist")
    }
  }
}
