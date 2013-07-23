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

package org.kiji.express.modeling.framework

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.KijiSlice
import org.kiji.express.avro.KvStoreType
import org.kiji.express.modeling.ExtractFn
import org.kiji.express.modeling.Extractor
import org.kiji.express.modeling.KeyValueStore
import org.kiji.express.modeling.ScoreFn
import org.kiji.express.modeling.Scorer
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.config.KVStore
import org.kiji.express.modeling.impl.AvroKVRecordKeyValueStore
import org.kiji.express.modeling.impl.AvroRecordKeyValueStore
import org.kiji.express.modeling.impl.KijiTableKeyValueStore
import org.kiji.express.util.ExpressGenericRow
import org.kiji.express.util.ExpressGenericTable
import org.kiji.express.util.Tuples
import org.kiji.mapreduce.KijiContext
import org.kiji.mapreduce.kvstore.{ KeyValueStore => JKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroRecordKeyValueStore => JAvroRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ KijiTableKeyValueStore => JKijiTableKeyValueStore }
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.mapreduce.produce.ProducerContext
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI

/**
 * A producer for running [[org.kiji.express.modeling.config.ModelDefinition]]s.
 *
 * This producer executes the extract and score phases of a model in series. The model that this
 * producer will run is loaded from the json configuration strings stored in configuration keys:
 * <ul>
 *   <li>`org.kiji.express.model.definition`</li>
 *   <li>`org.kiji.express.model.environment`</li>
 * </ul>
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class ExtractScoreProducer
    extends KijiProducer {
  /** The model definition. This variable must be initialized. */
  private[this] var _modelDefinition: Option[ModelDefinition] = None
  private[this] def modelDefinition: ModelDefinition = {
    _modelDefinition.getOrElse {
      throw new IllegalStateException(
          "ExtractScoreProducer is missing its model definition. Did setConf get called?")
    }
  }

  /** Environment required to run phases of a model. This variable must be initialized. */
  private[this] var _modelEnvironment: Option[ModelEnvironment] = None
  private[this] def modelEnvironment: ModelEnvironment = {
    _modelEnvironment.getOrElse {
      throw new IllegalStateException(
          "ExtractScoreProducer is missing its run profile. Did setConf get called?")
    }
  }

  /** Extractor to use for this model definition. This variable must be initialized. */
  private[this] var _extractor: Option[Extractor] = None
  private[this] def extractor: Extractor = {
    _extractor.getOrElse {
      throw new IllegalStateException(
          "ExtractScoreProducer is missing its extractor. Did setConf get called?")
    }
  }

  /** Scorer to use for this model definition. This variable must be initialized. */
  private[this] var _scorer: Option[Scorer] = None
  private[this] def scorer: Scorer = {
    _scorer.getOrElse {
      throw new IllegalStateException(
          "ExtractScoreProducer is missing its scorer. Did setConf get called?")
    }
  }

  /** Used to decode rows from Kiji with a generic API. This variable must be initialized. */
  private[this] var _genericTable: Option[ExpressGenericTable] = None
  private[this] def genericTable: ExpressGenericTable = {
    _genericTable.getOrElse {
      throw new IllegalStateException(
          "Generic Avro decoding facilities have not been initialized yet.")
    }
  }

  /**
   * Sets the Configuration for this KijiProducer to use. This function is guaranteed to be called
   * immediately after instantiation.
   *
   * This method loads a [[org.kiji.express.modeling.config.ModelDefinition]] and a
   * [[org.kiji.express.modeling.config.ModelEnvironment]] for ExtractScoreProducer to use.
   *
   * @param conf object that this producer should use.
   */
  override def setConf(conf: Configuration) {
    // Load model definition.
    val modelDefinitionJson: String = conf.get(ExtractScoreProducer.modelDefinitionConfKey)
    // scalastyle:off null
    require(
        modelDefinitionJson != null,
        "A ModelDefinition was not specified!")
    // scalastyle:on null
    val modelDefinitionDef = ModelDefinition.fromJson(modelDefinitionJson)
    _modelDefinition = Some(modelDefinitionDef)

    // Load run profile.
    val modelEnvironmentJson: String = conf.get(ExtractScoreProducer.modelEnvironmentConfKey)
    // scalastyle:off null
    require(
        modelEnvironmentJson != null,
        "A ModelEnvironment was not specified!")
    // scalastyle:on null
    val modelEnvironmentDef = ModelEnvironment.fromJson(modelEnvironmentJson)
    _modelEnvironment = Some(modelEnvironmentDef)

    // Make an instance of each requires phase.
    val extractor = modelDefinitionDef
        .extractorClass
        .newInstance()
        .asInstanceOf[Extractor]
    val scorer = modelDefinitionDef
        .scorerClass
        .newInstance()
        .asInstanceOf[Scorer]
    _extractor = Some(extractor)
    _scorer = Some(scorer)

    val uri = KijiURI.newBuilder(modelEnvironmentDef.modelTableUri).build()
    val columns: Seq[KijiColumnName] = modelEnvironmentDef
        .extractEnvironment
        .dataRequest
        .toKijiDataRequest()
        .getColumns
        .asScala
        .map { column => column.getColumnName() }
        .toSeq
    if (_genericTable.isDefined) {
      genericTable.close()
    }
    _genericTable = Some(new ExpressGenericTable(uri, conf, columns))

    // Finish setting the conf object.
    super.setConf(conf)
  }

  /**
   * Returns a KijiDataRequest that describes which input columns need to be available to the
   * producer. This method may be called multiple times, perhaps before `setup`.
   *
   * This method reads the Extract phase's data request configuration from this model's run profile
   * and builds a KijiDataRequest from it.
   *
   * @return a kiji data request.
   */
  override def getDataRequest(): KijiDataRequest = modelEnvironment
      .extractEnvironment
      .dataRequest.toKijiDataRequest()

  /**
   * Returns the name of the column this producer will write to.
   *
   * This method reads the Score phase's output column from this model's run profile and returns it.
   *
   * @return the output column name.
   */
  override def getOutputColumn(): String = modelEnvironment
      .scoreEnvironment
      .outputColumn

  /**
   * Opens the kvstores required for the extract and score phase. Reads kvstore configurations from
   * the provided model environment.
   *
   * @return a mapping from kvstore names to opened kvstores.
   */
  override def getRequiredStores(): java.util.Map[String, JKeyValueStore[_, _]] = {
    // Open the kvstores defined for the extract phase.
    val extractStoreDefs: Seq[KVStore] = modelEnvironment
        .extractEnvironment
        .kvstores
    val extractStores: Map[String, JKeyValueStore[_, _]] = ExtractScoreProducer
        .openJKvstores(extractStoreDefs, getConf(), "extract-")

    // Open the kvstores defined for the score phase.
    val scoreStoreDefs: Seq[KVStore] = modelEnvironment
        .scoreEnvironment
        .kvstores
    val scoreStores: Map[String, JKeyValueStore[_, _]] = ExtractScoreProducer
        .openJKvstores(scoreStoreDefs, getConf(), "score-")

    // Combine the opened kvstores.
    (extractStores ++ scoreStores)
        .asJava
  }

  override def setup(context: KijiContext) {
    // Setup the extract phase's kvstores.
    val extractStoreDefs: Seq[KVStore] = modelEnvironment
        .extractEnvironment
        .kvstores
    extractor.kvstores = ExtractScoreProducer
        .wrapKvstoreReaders(extractStoreDefs, context, "extract-")

    // Setup the score phase's kvstores.
    val scoreStoreDefs: Seq[KVStore] = modelEnvironment
        .scoreEnvironment
        .kvstores
    scorer.kvstores = ExtractScoreProducer
        .wrapKvstoreReaders(scoreStoreDefs, context, "score-")
  }

  override def produce(input: KijiRowData, context: ProducerContext) {
    val ExtractFn(extractFields, extract) = extractor.extractFn
    val ScoreFn(scoreFields, score) = scorer.scoreFn

    // Setup fields.
    val fieldMapping: Map[String, KijiColumnName] = modelEnvironment
        .extractEnvironment
        .fieldBindings
        .map { binding =>
          (binding.getTupleFieldName(), new KijiColumnName(binding.getStoreFieldName()))
        }
        .toMap
    val extractInputFields: Seq[String] = {
      // If the field specified is the wildcard field, use all columns referenced in this model
      // environment's field bindings.
      if (extractFields._1.isAll()) {
        fieldMapping.keys.toSeq
      } else {
        Tuples.fieldsToSeq(extractFields._1)
      }
    }
    val extractOutputFields: Seq[String] = {
      // If the field specified is the results field, use all input fields from the extract phase.
      if (extractFields._2.isResults()) {
        extractInputFields
      } else {
        Tuples.fieldsToSeq(extractFields._2)
      }
    }
    val scoreInputFields: Seq[String] = {
      // If the field specified is the wildcard field, use all fields output by the extract phase.
      if (scoreFields.isAll()) {
        extractOutputFields
      } else {
        Tuples.fieldsToSeq(scoreFields)
      }
    }

    // Wrap KijiRowData in ExpressGenericRow to permit decoding of cells into generic Avro records.
    val row: ExpressGenericRow = genericTable.getRow(input)

    // Prepare input to the extract phase.
    val slices: Seq[KijiSlice[Any]] = extractInputFields
        .map { field =>
          val columnName: KijiColumnName = fieldMapping(field.toString)

          // Build a slice from each column within the row.
          if (columnName.isFullyQualified) {
            KijiSlice[Any](row.iterator(columnName.getFamily(), columnName.getQualifier()))
          } else {
            KijiSlice[Any](row.iterator(columnName.getFamily()))
          }
        }

    // Get output from the extract phase.
    val featureVector: Product = Tuples.fnResultToTuple(
        extract(Tuples.tupleToFnArg(Tuples.seqToTuple(slices))))
    val featureMapping: Map[String, Any] = extractOutputFields
        .zip(featureVector.productIterator.toIterable)
        .toMap

    // Get a score from the score phase.
    val scoreInput: Seq[Any] = scoreInputFields
        .map { field => featureMapping(field) }
    val scoreValue: Any =
        score(Tuples.tupleToFnArg(Tuples.seqToTuple(scoreInput)))

    // Write the score out using the provided context.
    context.put(scoreValue)
  }

  override def cleanup(context: KijiContext) {
    if (_genericTable.isDefined) {
      genericTable.close()
    }
  }
}

object ExtractScoreProducer {
  /**
   * Configuration key addressing the JSON description of a
   * [[org.kiji.express.modeling.config.ModelDefinition]].
   */
  val modelDefinitionConfKey: String = "org.kiji.express.model.definition"

  /**
   * Configuration key addressing the JSON configuration of a
   * [[org.kiji.express.modeling.config.ModelEnvironment]].
   */
  val modelEnvironmentConfKey: String = "org.kiji.express.model.environment"

  /**
   * Wrap the provided kvstores in their scala counterparts.
   *
   * @param kvstores to open.
   * @param context providing access to the opened kvstores.
   * @param prefix prepended to the provided kvstore names.
   * @return a mapping from the kvstore's name to the wrapped kvstore.
   */
  def wrapKvstoreReaders(
      kvstores: Seq[KVStore],
      context: KijiContext,
      prefix: String = ""
  ): Map[String, KeyValueStore[_, _]] = {
    return kvstores
        .map { kvstore: KVStore =>
          val jkvstoreReader = context.getStore(prefix + kvstore.name)
          val wrapped: KeyValueStore[_, _] = KvStoreType.valueOf(kvstore.storeType) match {
            case KvStoreType.AVRO_KV => new AvroKVRecordKeyValueStore(jkvstoreReader)
            case KvStoreType.AVRO_RECORD => new AvroRecordKeyValueStore(jkvstoreReader)
            case KvStoreType.KIJI_TABLE => new KijiTableKeyValueStore(jkvstoreReader)
          }
          (kvstore.name, wrapped)
        }
        .toMap
  }

  /**
   * Open the provided kvstore definitions.
   *
   * @param kvstores to open.
   * @param conf containing settings pertaining to the specified kvstores.
   * @param prefix to prepend to the provided kvstore names.
   * @return a mapping from the kvstore's name to the opened kvstore.
   */
  def openJKvstores(
      kvstores: Seq[KVStore],
      conf: Configuration,
      prefix: String = ""): Map[String, JKeyValueStore[_, _]] = {
    kvstores
        // Open the kvstores defined for the extract phase.
        .map { kvstore: KVStore =>
          val properties = kvstore.properties

          // Handle each type of kvstore differently.
          val jkvstore: JKeyValueStore[_, _] = KvStoreType.valueOf(kvstore.storeType) match {
            case KvStoreType.AVRO_KV => {

              // Open AvroKV.
              val builder = JAvroKVRecordKeyValueStore
                  .builder()
                  .withConfiguration(conf)
                  .withInputPath(new Path(properties("path")))
              if (properties.contains("use_dcache")) {
                builder
                    .withDistributedCache(properties("use_dcache") == "true")
                    .build()
              } else {
                builder.build()
              }
            }
            case KvStoreType.AVRO_RECORD => {
              // Open AvroRecord.
              val builder = JAvroRecordKeyValueStore
                  .builder()
                  .withConfiguration(conf)
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
            case KvStoreType.KIJI_TABLE => {
              // Kiji table.
              val uri: KijiURI = KijiURI.newBuilder(properties("uri")).build()
              val columnName: KijiColumnName = new KijiColumnName(properties("column"))
              JKijiTableKeyValueStore
                  .builder()
                  .withConfiguration(conf)
                  .withTable(uri)
                  .withColumn(columnName.getFamily(), columnName.getQualifier())
                  .build()
            }
            case kvstoreType => throw new UnsupportedOperationException(
                "KeyValueStores of type \"%s\" are not supported".format(kvstoreType.toString))
          }

          // Pack the kvstore into a tuple with its name.
          (prefix + kvstore.name, jkvstore)
        }
        .toMap
  }
}
