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

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.avro.AvroKijiOutputSpec
import org.kiji.express.avro.AvroKijiSingleColumnOutputSpec
import org.kiji.express.avro.AvroOutputSpec
/**
 * Represents the configuration for an output data source for a phase of the model lifecycle.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait OutputSpec {
  /**
   * Creates an Avro OutputSpec from this specification.
   *
   * @return an Avro OutputSpec from this specification.
   */
  private[express] def toAvroOutputSpec(): AvroOutputSpec
}

/**
 * Configuration necessary to use a Kiji table as a data sink.
 *
 * @param tableUri addressing the Kiji table that this output spec will write from.
 * @param timeStampField the tuple field for the timestamp associated with the output.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how output fields are mapped onto columns in a Kiji table.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class KijiOutputSpec(
    tableUri: String,
    fieldBindings: Seq[FieldBinding],
    timeStampField: Option[String] = None) extends OutputSpec{
  private[express] override def toAvroOutputSpec(): AvroOutputSpec = {
    val avroKijiOutputSpec = AvroKijiOutputSpec
        .newBuilder()
        .setTableUri(tableUri)
        .setTimeStampField(timeStampField.getOrElse(null))
        .setFieldBindings(fieldBindings.map { _.toAvroFieldBinding } .asJava)
        .build()

    AvroOutputSpec
        .newBuilder()
        .setSpecification(avroKijiOutputSpec)
        .build()
  }
}

/**
 * The companion object to KijiOutputSpec for factory methods.
 */
object KijiOutputSpec {
  /**
   * Converts an Avro KijiOutputSpec specification into a KijiOutputSpec case class.
   *
   * @param avroKijiOutputSpec is the Avro specification.
   * @return the avro KijiOutputSpec specification as a KijiOutputSpec case class.
   */
  private[express] def apply(avroKijiOutputSpec: AvroKijiOutputSpec): KijiOutputSpec = {
    KijiOutputSpec(
        tableUri = avroKijiOutputSpec.getTableUri,
        timeStampField = Option(avroKijiOutputSpec.getTimeStampField),
        fieldBindings = avroKijiOutputSpec.getFieldBindings.asScala.map { FieldBinding(_) })
  }
}

/**
 * Configuration necessary to use a Kiji table column as the output. Used in the score phase.
 *
 * @param tableUri addressing the Kiji table to write to.
 * @param outputColumn specifies the Kiji column for the output of the score phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class KijiSingleColumnOutputSpec(
    tableUri: String,
    outputColumn: String) extends OutputSpec{
  private[express] override def toAvroOutputSpec(): AvroOutputSpec = {
    val avroScorePhaseOutputSpec = AvroKijiSingleColumnOutputSpec
      .newBuilder()
      .setTableUri(tableUri)
      .setOutputColumn(outputColumn)
      .build()

    AvroOutputSpec
      .newBuilder()
      .setSpecification(avroScorePhaseOutputSpec)
      .build()
  }
}

/**
 * The companion object to KijiSingleColumnOutputSpec for factory methods.
 */
object KijiSingleColumnOutputSpec {
  /**
   * Converts an AvroKijiSingleColumnOutputSpec specification into a KijiSingleColumnOutputSpec
   * case class.
   *
   * @param avroScorePhaseOutputSpec is the Avro specification.
   * @return the avro KijiSingleColumnOutputSpec specification as a KijiOutputSpec case class.
   */
  private[express] def apply(avroScorePhaseOutputSpec: AvroKijiSingleColumnOutputSpec):
      KijiSingleColumnOutputSpec = {
    KijiSingleColumnOutputSpec(
      tableUri = avroScorePhaseOutputSpec.getTableUri,
      outputColumn = avroScorePhaseOutputSpec.getOutputColumn)
  }
}
