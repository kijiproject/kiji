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
import org.kiji.express.avro.AvroInputSpec
import org.kiji.express.avro.AvroKijiInputSpec

/**
 * Represents the configuration for an input data source for a phase of the model lifecycle.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait InputSpec {
  /**
   * Creates an Avro InputSpec from this specification.
   *
   * @return an Avro InputSpec from this specification.
   */
  private[express] def toAvroInputSpec(): AvroInputSpec
}

/**
 * Configuration necessary to use a Kiji table as a data source.
 *
 * @param tableUri addressing the Kiji table that this input spec will read from.
 * @param dataRequest describing the input columns required by this input spec.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how data that is requested for the extract phase is mapped onto named
 *     input fields.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class KijiInputSpec(
    tableUri: String,
    dataRequest: ExpressDataRequest,
    fieldBindings: Seq[FieldBinding]) extends InputSpec {
  private[express] override def toAvroInputSpec(): AvroInputSpec = {
    val avroKijiInputSpec = AvroKijiInputSpec
        .newBuilder()
        .setTableUri(tableUri)
        .setDataRequest(dataRequest.toAvro)
        .setFieldBindings(fieldBindings.map { _.toAvroFieldBinding } .asJava)
        .build()

    AvroInputSpec
        .newBuilder()
        .setSpecification(avroKijiInputSpec)
        .build()
  }
}

/**
 * The companion object to KijiInputSpec for factory methods.
 */
object KijiInputSpec {
  /**
   * Converts an Avro KijiInputSpec specification into a KijiInputSpec case class.
   *
   * @param avroKijiInputSpec is the Avro specification.
   * @return the avro KijiInputSpec specification as a KijiInputSpec case class.
   */
  private[express] def apply(avroKijiInputSpec: AvroKijiInputSpec): KijiInputSpec = {
    KijiInputSpec(
        tableUri = avroKijiInputSpec.getTableUri,
        dataRequest = ExpressDataRequest(avroKijiInputSpec.getDataRequest),
        fieldBindings = avroKijiInputSpec.getFieldBindings.asScala.map { FieldBinding(_) })
  }
}
