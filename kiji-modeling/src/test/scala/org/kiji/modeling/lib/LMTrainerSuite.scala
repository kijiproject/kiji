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

package org.kiji.modeling.lib

import java.io.File

import com.google.common.io.Files
import com.twitter.scalding.Args
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.modeling.config.ExpressColumnRequest
import org.kiji.modeling.config.ExpressDataRequest
import org.kiji.modeling.config.FieldBinding
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.TextSourceSpec
import org.kiji.modeling.config.TrainEnvironment
import org.kiji.modeling.framework.ModelExecutor
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.util.InstanceBuilder

/**
 * Tests the Linear Regression Trainer.
 */
@RunWith(classOf[JUnitRunner])
class LMTrainerSuite extends KijiSuite {
  /**
   * Tests that LMTrainer can calculate the equation for line y = x.
   */
  test("A Linear Regression trainer works correctly") {
    val inputTable: String = "two-double-columns.json"
    val paramsFile: String = "src/test/resources/sources/LRparams"
    val outputParams: String = Files.createTempDir().getAbsolutePath

    val testLayoutDesc: TableLayoutDesc = layout(inputTable).getDesc
    testLayoutDesc.setName("lr_table")

    val kiji: Kiji = new InstanceBuilder("default")
      .withTable(testLayoutDesc)
      .withRow("row1")
      .withFamily("family")
      .withQualifier("column1").withValue(0.0)
      .withQualifier("column2").withValue(0.0)
      .withRow("row2")
      .withFamily("family")
      .withQualifier("column1").withValue(1.0)
      .withQualifier("column2").withValue(1.0)
      .withRow("row3")
      .withFamily("family")
      .withQualifier("column1").withValue(2.0)
      .withQualifier("column2").withValue(2.0)
      .build()

    val modelDefinition: ModelDefinition = ModelDefinition(
      name = "lr-model-def",
      version = "1.0",
      trainerClass = Some(classOf[LMTrainer]))

    val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
        Seq(new ExpressColumnRequest("family:column1", 1, None),
            new ExpressColumnRequest("family:column2", 1, None)))

    doAndRelease(kiji.openTable("lr_table")) { table: KijiTable =>
      val tableUri: KijiURI = table.getURI()

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
          name = "lr-train-model-environment",
          version = "1.0",
          trainEnvironment = Some(TrainEnvironment(
              inputSpec = Map(
                  "dataset" -> KijiInputSpec(
                      tableUri.toString,
                      dataRequest = request,
                      fieldBindings = Seq(
                          FieldBinding(tupleFieldName = "attributes",
                              storeFieldName = "family:column1"),
                          FieldBinding(tupleFieldName = "target",
                              storeFieldName = "family:column2"))
                  ),
                  "parameters" -> TextSourceSpec(
                      path = paramsFile
                  )
              ),
              outputSpec = Map(
                  "parameters" -> TextSourceSpec(
                      path = outputParams
              )),
              keyValueStoreSpecs = Seq()
          ))
      )

      // Build the produce job.
      val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)

      // Verify that everything went as expected.
      assert(modelExecutor.runTrainer(new Args(Map("max-iter" -> List("1")))))
    }
    kiji.release()
    val lines = doAndClose(scala.io.Source.fromFile(outputParams + "/part-00000")) {
      source: scala.io.Source => source.mkString
    }

    // Theta values after a single iteration.
    assert(lines.split("""\s+""").map(_.toDouble).deep ===
        Array(0.0, 0.75, 3.0, 1.0, 1.25, 5.0).deep)
    FileUtils.deleteDirectory(new File(outputParams))
  }
}
