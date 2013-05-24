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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.ExpressGenericRow
import org.kiji.express.ExpressGenericTable
import org.kiji.express.KijiSlice
import org.kiji.express.avro.ColumnSpec
import org.kiji.mapreduce.KijiContext
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.mapreduce.produce.ProducerContext
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI

/**
 * A producer for running [[org.kiji.express.modeling.ModelDefinition]]s.
 *
 * This producer executes the extract and score phases of a model in series. The model that this
 * producer will run is loaded from the json configuration strings stored in configuration keys:
 * <ul>
 *   <li>`org.kiji.express.modeling.definition`</li>
 *   <li>`org.kiji.express.modeling.environment`</li>
 * </ul>
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class ExtractScoreProducer
    extends KijiProducer {
  /** Definition of a Model Pipeline. This variable must be initialized. */
  private[this] var _modelDefinition: Option[ModelDefinition] = None
  private[this] def modelDefinition: ModelDefinition = {
    _modelDefinition.getOrElse {
      throw new IllegalStateException(
          "ExtractScoreProducer is missing its model pipeline. Did setConf get called?")
    }
  }

  /** Configuration required to run a Model Pipeline. This variable must be initialized. */
  private[this] var _modelEnvironment: Option[ModelEnvironment] = None
  private[this] def modelEnvironment: ModelEnvironment = {
    _modelEnvironment.getOrElse {
      throw new IllegalStateException(
          "ExtractScoreProducer is missing its run profile. Did setConf get called?")
    }
  }

  /** Extractor to use for this Model Pipeline. This variable must be initialized. */
  private[this] var _extractor: Option[Extractor] = None
  private[this] def extractor: Extractor = {
    _extractor.getOrElse {
      throw new IllegalStateException(
          "ExtractScoreProducer is missing its extractor. Did setConf get called?")
    }
  }

  /** Scorer to use for this Model Pipeline. This variable must be initialized. */
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
   * Sets the Configuration for this KijiProducer to use.  This function is guaranteed to be called
   * immediately after instantiation.
   *
   * This method loads a [[org.kiji.express.modeling.ModelDefinition]] and a
   * [[org.kiji.express.modeling.ModelEnvironment]] for ExtractScoreProducer to use.
   *
   * @param conf object that this producer should use.
   */
  override def setConf(conf: Configuration) {
    // Load model pipeline.
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
        .getColumns
        .asScala
        .map { column => column.getColumnName() }
        .toSeq
    if (_genericTable.isDefined) {
      genericTable.close()
    }
    _genericTable = Some(new ExpressGenericTable(uri, columns))

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
      .dataRequest

  /**
   * Returns the name of the column this producer will write to.
   *
   * This method reads the Score phase's output column from this model's run profile and returns it.
   *
   * @return the output column name.
   */
  override def getOutputColumn(): String = modelEnvironment.scoreEnvironment.outputColumn

  override def produce(input: KijiRowData, context: ProducerContext) {
    val ExtractFn(extractFields, extract) = extractor.extractFn
    val (extractInputFields, extractOutputFields) = extractFields
    val ScoreFn(scoreFields, score) = scorer.scoreFn

    val fieldMapping = modelEnvironment
        .extractEnvironment
        .fieldBindings
        .map { binding => (binding.getTupleFieldName(), binding.getStoreFieldName()) }
        .toMap

    val row: ExpressGenericRow = genericTable.getRow(input)

    // Get the data required by the extract phase out of the row data.
    val slices: Seq[KijiSlice[Any]] = extractInputFields
        .iterator()
        .asScala
        .map { field =>
          val columnName = new KijiColumnName(fieldMapping(field.toString))

          // Build a slice from each column within the row.
          if (columnName.isFullyQualified) {
            KijiSlice[Any](row.iterator(columnName.getFamily(), columnName.getQualifier()))
          } else {
            KijiSlice[Any](row.iterator(columnName.getFamily()))
          }
        }
        .toSeq

    // Get a feature vector from the extract phase.
    val featureVector: Product = ExtractScoreProducer.fnResultToTuple(
        extract(ExtractScoreProducer.tupleToFnArg(ExtractScoreProducer.seqToTuple(slices))))
    val featureMapping: Map[String, Any] = extractOutputFields
        .iterator
        .asScala
        .map { field => field.toString }
        .zip(featureVector.productIterator)
        .toMap

    // Get a score from the score phase.
    val scoreInput: Seq[Any] = scoreFields
        .iterator
        .asScala
        .map { field => featureMapping(field.toString) }
        .toSeq
    val scoreValue: Any =
        score(ExtractScoreProducer.tupleToFnArg(ExtractScoreProducer.seqToTuple(scoreInput)))

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
   * [[org.kiji.express.modeling.ModelDefinition]].
   */
  val modelDefinitionConfKey: String = "org.kiji.express.modeling.pipeline"

  /**
   * Configuration key addressing the JSON configuration of a
   * [[org.kiji.express.modeling.ModelEnvironment]].
   */
  val modelEnvironmentConfKey: String = "org.kiji.express.modeling.runprofile"

  /**
   * Converts a tuple into an appropriate representation for processing by a model phase function.
   * Handles instances of Tuple1 as special cases and unpacks them to permit functions with only one
   * parameter to be defined without expecting their argument to be wrapped in a Tuple1 instance.
   *
   * @tparam T is the type of the output function argument.
   * @param tuple to convert.
   * @return an argument ready to be passed to a model phase function.
   */
  def tupleToFnArg[T](tuple: Product): T = {
    tuple match {
      case Tuple1(x1) => x1.asInstanceOf[T]
      case other => other.asInstanceOf[T]
    }
  }

  /**
   * Converts a function return value into a tuple. Handles the case where the provided result is
   * not a tuple by wrapping it in a Tuple1 instance.
   *
   * @param result from a model phase function.
   * @return a processed tuple.
   */
  def fnResultToTuple(result: Any): Product = {
    result match {
      case tuple: Tuple1[_] => tuple
      case tuple: Tuple2[_, _] => tuple
      case tuple: Tuple3[_, _, _] => tuple
      case tuple: Tuple4[_, _, _, _] => tuple
      case tuple: Tuple5[_, _, _, _, _] => tuple
      case tuple: Tuple6[_, _, _, _, _, _] => tuple
      case tuple: Tuple7[_, _, _, _, _, _, _] => tuple
      case tuple: Tuple8[_, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple9[_, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple10[_, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple11[_, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple12[_, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple13[_, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple14[_, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case other => Tuple1(other)
    }
  }

  /**
   * Converts a sequence to a tuple.
   *
   * @tparam T is the type of the output tuple.
   * @param sequence to convert.
   * @return a tuple converted from the provided sequence.
   */
  def seqToTuple[T <: Product](sequence: Seq[_]): T = {
    val tuple = sequence match {
      case Seq(x1) => {
        Tuple1(x1)
      }
      case Seq(x1, x2) => {
        Tuple2(x1, x2)
      }
      case Seq(x1, x2, x3) => {
        Tuple3(x1, x2, x3)
      }
      case Seq(x1, x2, x3, x4) => {
        Tuple4(x1, x2, x3, x4)
      }
      case Seq(x1, x2, x3, x4, x5) => {
        Tuple5(x1, x2, x3, x4, x5)
      }
      case Seq(x1, x2, x3, x4, x5, x6) => {
        Tuple6(x1, x2, x3, x4, x5, x6)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7) => {
        Tuple7(x1, x2, x3, x4, x5, x6, x7)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8) => {
        Tuple8(x1, x2, x3, x4, x5, x6, x7, x8)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9) => {
        Tuple9(x1, x2, x3, x4, x5, x6, x7, x8, x9)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) => {
        Tuple10(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11) => {
        Tuple11(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12) => {
        Tuple12(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13) => {
        Tuple13(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14) => {
        Tuple14(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15) => {
        Tuple15(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16) => {
        Tuple16(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17) => {
        Tuple17(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18) => {
        Tuple18(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19) => {
        Tuple19(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19, x20) => {
        Tuple20(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19, x20, x21) => {
        Tuple21(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20, x21)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19, x20, x21, x22) => {
        Tuple22(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20, x21, x22)
      }
    }

    tuple.asInstanceOf[T]
  }
}
