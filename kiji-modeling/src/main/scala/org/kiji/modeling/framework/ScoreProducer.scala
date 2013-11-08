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

import org.apache.hadoop.conf.Configuration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.flow.ColumnRequestInput
import org.kiji.express.flow.ColumnRequestOutput
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.util.GenericRowDataConverter
import org.kiji.express.util.Tuples
import org.kiji.mapreduce.KijiContext
import org.kiji.mapreduce.kvstore.{ KeyValueStore => JKeyValueStore }
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.mapreduce.produce.ProducerContext
import org.kiji.modeling.ExtractFn
import org.kiji.modeling.Extractor
import org.kiji.modeling.ScoreFn
import org.kiji.modeling.Scorer
import org.kiji.modeling.config.KeyValueStoreSpec
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.impl.ModelJobUtils
import org.kiji.modeling.impl.ModelJobUtils.PhaseType.SCORE
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI

/**
 * A producer for running [[org.kiji.modeling.config.ModelDefinition]]s.
 *
 * This producer executes the score phase of a model. The model that this producer will run is
 * loaded from the json configuration strings stored in configuration keys:
 * <ul>
 *   <li>`org.kiji.express.model.definition`</li>
 *   <li>`org.kiji.express.model.environment`</li>
 * </ul>
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
final class ScoreProducer
    extends KijiProducer {
  /** The model definition. This variable must be initialized. */
  private[this] var _modelDefinition: Option[ModelDefinition] = None
  private[this] def modelDefinition: ModelDefinition = {
    _modelDefinition.getOrElse {
      throw new IllegalStateException(
          "ScoreProducer is missing its model definition. Did setConf get called?")
    }
  }

  /** Environment required to run phases of a model. This variable must be initialized. */
  private[this] var _modelEnvironment: Option[ModelEnvironment] = None
  private[this] def modelEnvironment: ModelEnvironment = {
    _modelEnvironment.getOrElse {
      throw new IllegalStateException(
          "ScoreProducer is missing its run profile. Did setConf get called?")
    }
  }

  /** Extractor to use for this model definition. This variable must be initialized. */
  private[this] var _extractor: Option[Extractor] = None
  private[this] def extractor: Option[Extractor] = {
    _extractor
  }

  /** Scorer to use for this model definition. This variable must be initialized. */
  private[this] var _scorer: Option[Scorer] = None
  private[this] def scorer: Scorer = {
    _scorer.getOrElse {
      throw new IllegalStateException(
          "ScoreProducer is missing its scorer. Did setConf get called?")
    }
  }

  /** A converter that configured row data to decode data generically. */
  private[this] var _rowConverter: Option[GenericRowDataConverter] = None
  private[this] def rowConverter: GenericRowDataConverter = {
    _rowConverter.getOrElse {
      throw new IllegalStateException("ExtractScoreProducer is missing its row data converter. "
          + "Was setup() called?")
    }
  }

  /**
   * Sets the Configuration for this KijiProducer to use. This function is guaranteed to be called
   * immediately after instantiation.
   *
   * This method loads a [[org.kiji.modeling.config.ModelDefinition]] and a
   * [[org.kiji.modeling.config.ModelEnvironment]] for ScoreProducer to use.
   *
   * @param conf object that this producer should use.
   */
  override def setConf(conf: Configuration) {
    // Load model definition.
    val modelDefinitionJson: String = conf.get(ScoreProducer.modelDefinitionConfKey)
    // scalastyle:off null
    require(
        modelDefinitionJson != null,
        "A ModelDefinition was not specified!")
    // scalastyle:on null
    val modelDefinitionDef = ModelDefinition.fromJson(modelDefinitionJson)
    _modelDefinition = Some(modelDefinitionDef)

    // Load run profile.
    val modelEnvironmentJson: String = conf.get(ScoreProducer.modelEnvironmentConfKey)
    // scalastyle:off null
    require(
        modelEnvironmentJson != null,
        "A ModelEnvironment was not specified!")
    // scalastyle:on null
    val modelEnvironmentDef = ModelEnvironment.fromJson(modelEnvironmentJson)
    _modelEnvironment = Some(modelEnvironmentDef)

    // Make an instance of each requires phase.
    _extractor = modelDefinitionDef
        .scoreExtractorClass
        .map { _.newInstance() }

    val scorer = modelDefinitionDef
        .scorerClass
        .get
        .newInstance()
    _scorer = Some(scorer)

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
  override def getDataRequest: KijiDataRequest = ModelJobUtils
      .getDataRequest(modelEnvironment, SCORE)
      .get

  /**
   * Returns the name of the column this producer will write to.
   *
   * This method reads the Score phase's output column from this model's run profile and returns it.
   *
   * @return the output column name.
   */
  override def getOutputColumn: String = ModelJobUtils
      .getOutputColumn(modelEnvironment)

  /**
   * Opens the key value stores required for the extract and score phase. Reads key value store spec
   * configurations from the provided model environment.
   *
   * @return a mapping from keyValueStoreSpec names to opened key value stores.
   */
  override def getRequiredStores: java.util.Map[String, JKeyValueStore[_, _]] = {
    // Open the key value stores defined for the score phase.
    val scoreStoreDefs: Seq[KeyValueStoreSpec] = modelEnvironment
        .scoreEnvironment
        .get
        .keyValueStoreSpecs
    val scoreStores: Map[String, JKeyValueStore[_, _]] = ModelJobUtils
        .openJKvstores(scoreStoreDefs, getConf)
    scoreStores.asJava
  }

  override def cleanup(context: KijiContext) {
    rowConverter.close()
  }

  override def setup(context: KijiContext) {
    // Setup the score phase's key value stores.
    val scoreStoreDefs: Seq[KeyValueStoreSpec] = modelEnvironment
        .scoreEnvironment
        .get
        .keyValueStoreSpecs
    scorer.keyValueStores = ModelJobUtils
        .wrapKvstoreReaders(scoreStoreDefs, context)
    extractor.map { e => e.keyValueStores = scorer.keyValueStores }

    // Setup the row converter.
    val uriString = modelEnvironment
        .scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]
        .tableUri
    val uri = KijiURI.newBuilder(uriString).build()
    _rowConverter = Some(new GenericRowDataConverter(uri, getConf))
  }

  override def produce(input: KijiRowData, context: ProducerContext) {
    val ScoreFn(scoreFields, score) = scorer.scoreFn

    // Setup fields.
    val fieldMapping: Map[String, KijiColumnName] = modelEnvironment
        .scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]
        .columnsToFields
        .toList
        // List of (ColumnRequestInput, Symbol) pairs
        .map { case (column: ColumnRequestInput, field: Symbol) => {
          (field.name, column.columnName)
        }}
        .toMap

    // Configure the row data input to decode its data generically.
    val row = rowConverter(input)

    // Prepare input to the extract phase.
    def getSlices(inputFields: Seq[String]): Seq[Any] = inputFields
        .map { (field: String) =>
          if (field == KijiScheme.entityIdField) {
            val uri = KijiURI
              .newBuilder(modelEnvironment.scoreEnvironment.get.inputSpec.tableUri)
              .build()
            EntityId.fromJavaEntityId(row.getEntityId())
          } else {
            val columnName: KijiColumnName = fieldMapping(field.toString)

            // Build a slice from each column within the row.
            if (columnName.isFullyQualified) {
              KijiSlice[Any](row, columnName.getFamily, columnName.getQualifier)
            } else {
              KijiSlice[Any](row, columnName.getFamily)
            }
          }
        }

    val extractFnOption: Option[ExtractFn[_, _]] = extractor.map { _.extractFn }
    val scoreInput = extractFnOption match {

      // If there is an extractor, use its extractFn to set up the correct input and output fields
      case Some(ExtractFn(extractFields, extract)) => {
        val extractInputFields: Seq[String] = {
          // If the field specified is the wildcard field, use all columns referenced in this model
          // environment's field bindings.
          if (extractFields._1.isAll) {
            fieldMapping.keys.toSeq
          } else {
            Tuples.fieldsToSeq(extractFields._1)
          }
        }
        val extractOutputFields: Seq[String] = {
          // If the field specified is the results field, use all input fields from the extract
          // phase.
          if (extractFields._2.isResults) {
            extractInputFields
          } else {
            Tuples.fieldsToSeq(extractFields._2)
          }
        }

        val scoreInputFields: Seq[String] = {
          // If the field specified is the wildcard field, use all fields output by the extract
          // phase.
          if (scoreFields.isAll) {
            extractOutputFields
          } else {
            Tuples.fieldsToSeq(scoreFields)
          }
        }

        // Prepare input to the extract phase.
        val slices = getSlices(extractInputFields)

        // Get output from the extract phase.
        val featureVector: Product = Tuples.fnResultToTuple(
          extract(Tuples.tupleToFnArg(Tuples.seqToTuple(slices))))
        val featureMapping: Map[String, Any] = extractOutputFields
            .zip(featureVector.productIterator.toIterable)
            .toMap

        // Get a score from the score phase.
        val scoreInput: Seq[Any] = scoreInputFields
          .map { field => featureMapping(field) }

        scoreInput
      }
      // If there's no extractor, use default input and output fields
      case None => {
        val scoreInputFields = Tuples.fieldsToSeq(scoreFields)
        val inputOutputFields = fieldMapping.keys.toSeq
        val slices = getSlices(inputOutputFields)

        // Get output from the extract phase.
        val featureVector: Product = Tuples.seqToTuple(slices)
        val featureMapping: Map[String, Any] = inputOutputFields
          .zip(featureVector.productIterator.toIterable)
          .toMap

        // Get a score from the score phase.
        val scoreInput: Seq[Any] = scoreInputFields
          .map { field => featureMapping(field) }

        scoreInput
      }
    }

    val scoreValue: Any =
        score(Tuples.tupleToFnArg(Tuples.seqToTuple(scoreInput)))

    // Write the score out using the provided context.
    context.put(scoreValue)
  }
}

object ScoreProducer {
  /**
   * Configuration key addressing the JSON description of a
   * [[org.kiji.modeling.config.ModelDefinition]].
   */
  val modelDefinitionConfKey: String = "org.kiji.express.model.definition"

  /**
   * Configuration key addressing the JSON configuration of a
   * [[org.kiji.modeling.config.ModelEnvironment]].
   */
  val modelEnvironmentConfKey: String = "org.kiji.express.model.environment"
}
