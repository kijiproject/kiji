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

import com.twitter.scalding.{TextLine, Args}
import org.kiji.express.flow.{ColumnFamilyOutputSpec, EntityId, KijiOutput}
import org.kiji.modeling.framework.KijiModelingJob

class OrderImporter(args: Args) extends KijiModelingJob(args) {
  val inputFileName: String = args("input")
  val outputUri: String = args("output")

  TextLine(inputFileName)
      .read
      .mapTo('line -> ('product_id, 'time_id, 'customer_id)) {
        line: String => {
          val contents: Array[String] = line.split("\t")
          (contents(0), contents(1).toInt, contents(2).toLong)
        }
      }
      .map(('customer_id, 'time_id) -> 'entityId) {
        components : (Long, Int) => EntityId(components._1, components._2)
      }
      .project('entityId, 'product_id)
      .insert('itemCount, 1)
      .write(KijiOutput(outputUri,
          Map('itemCount -> ColumnFamilyOutputSpec.builder
              .withFamily("purchased_items")
              .withQualifierSelector('product_id)
              .build)))
}
