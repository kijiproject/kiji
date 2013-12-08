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

package org.kiji.modeling.tool

import java.io.File

import scala.io.Source

import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.util.Resources.doAndClose
import org.kiji.express.flow.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class ModelLifecycleToolSuite extends KijiSuite {
  test("ModelLifecycleTool works properly") {
    val inputTable: String = "two-double-columns.json"
    val modelEnvPath: String = "src/test/resources/modelEnvironments/lm-trainer-env.json"
    val modelDefPath: String = "src/test/resources/modelDefinitions/lm-trainer-def.json"
    val outputFilePath: String = "/tmp/1380326524231-0"
    val tempFile: File = File.createTempFile("modelenv", ".json")

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

    // We need to reformat and write to a temp file because InstanceBuilder
    // creates an instance with a randomly named cluster e.g. fake.0
    // This means there is no way of knowning what the table's URI should be
    // when the test is run.
    val tableUri: String = doAndRelease(kiji.openTable("lr_table")) {
      table: KijiTable => table.getURI().toString
    }
    val modelEnv: String = doAndClose(scala.io.Source.fromFile(modelEnvPath)) {
      source: scala.io.Source => source.mkString
    }
    FileUtils.writeStringToFile(tempFile, modelEnv.format(tableUri))

    ModelLifecycleTool.main(Array("--model-def", modelDefPath,
        "--model-env", tempFile.getAbsolutePath,
        "--hdfs",
        "--max-iter", "1",
        "--skip-prepare"))

    kiji.release()
    val lines: String = doAndClose(Source.fromFile(outputFilePath + "/part-00000")) { source: Source
        => source.mkString }
    // Theta values after a single iteration.
    assert(lines.split("""\s+""").map(_.toDouble).deep ===
      Array(0.0, 0.75, 3.0, 1.0, 1.25, 5.0).deep)
    FileUtils.deleteDirectory(new File(outputFilePath))
    FileUtils.deleteQuietly(tempFile)
  }
}
