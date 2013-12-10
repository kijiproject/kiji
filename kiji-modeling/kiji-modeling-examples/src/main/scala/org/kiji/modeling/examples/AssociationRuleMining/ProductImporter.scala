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

package org.kiji.modeling.examples.AssociationRuleMining

import com.twitter.scalding.{FieldConversions, TextLine, Args}
import org.kiji.express.flow._
import com.twitter.scalding.TextLine

class ProductImporter(args: Args) extends KijiJob(args) with FieldConversions {
  val inputFileName: String = args("input")
  val outputUri: String = args("output")

  TextLine(inputFileName)
      .read
      .mapTo('line -> ('product_class_id, 'entityId, 'brand_name, 'product_name, 'SKU, 'SRP)) {
        line: String => {
          val contents: Array[String] = line.split("\t")
          (contents(0).toInt, EntityId(contents(1)), contents(2), contents(3),
              contents(4).toLong, contents(5).toFloat)
        }
      }
      .write(KijiOutput(outputUri,
          Map('product_class_id -> QualifiedColumnOutputSpec.builder
              .withColumn("info", "product_class_id")
              .build,
          'brand_name -> QualifiedColumnOutputSpec.builder
              .withColumn("info", "brand_name")
              .build,
          'product_name -> QualifiedColumnOutputSpec.builder
              .withColumn("info", "product_name")
              .build,
          'SKU -> QualifiedColumnOutputSpec.builder
              .withColumn("info", "sku")
              .build,
          'SRP -> QualifiedColumnOutputSpec.builder
              .withColumn("info", "srp")
              .build)))
}
