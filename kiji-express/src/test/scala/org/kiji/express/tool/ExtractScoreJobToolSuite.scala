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

package org.kiji.express.tool

import java.io.File
import java.io.FileWriter

import com.google.common.io.Files

import org.kiji.express.avro.FieldBinding
import org.kiji.express.KijiSuite
import org.kiji.express.modeling.ExtractEnvironment
import org.kiji.express.modeling.ExtractScoreProducerSuite
import org.kiji.express.modeling.ModelDefinition
import org.kiji.express.modeling.ModelEnvironment
import org.kiji.express.modeling.ScoreEnvironment
import org.kiji.express.Resources.doAndClose
import org.kiji.express.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

class ExtractScoreJobToolSuite extends KijiSuite {
  test("ExtractScoreJobTool can run a job.") {
    val tmpDir: File = Files.createTempDir()
    val modelDefFile: File = new File(tmpDir, "model-def.json")
    val modelEnvFile: File = new File(tmpDir, "model-env.json")

    try {
      val testLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

      val kiji: Kiji = new InstanceBuilder("default")
        .withTable(testLayout.getName(), testLayout)
        .withRow("row1")
        .withFamily("family")
        .withQualifier("column1").withValue("foo")
        .withRow("row2")
        .withFamily("family")
        .withQualifier("column1").withValue("bar")
        .build()

      doAndRelease(kiji.openTable(testLayout.getName())) { table: KijiTable =>
        val uri: KijiURI = table.getURI()

        // Update configuration object with appropriately serialized ModelDefinition/Environment
        // JSON.
        val request: KijiDataRequest = KijiDataRequest.create("family", "column1")
        val modelDefinition: ModelDefinition = new ModelDefinition(
          name = "test-model-definition",
          version = "1.0",
          extractorClass = classOf[ExtractScoreProducerSuite.DoublingExtractor],
          scorerClass = classOf[ExtractScoreProducerSuite.UpperCaseScorer])
        doAndClose(new FileWriter(modelDefFile)) { writer =>
          writer.write(modelDefinition.toJson())
        }

        val modelEnvironment: ModelEnvironment = new ModelEnvironment(
          name = "test-model-environment",
          version = "1.0",
          modelTableUri = uri.toString,
          extractEnvironment = new ExtractEnvironment(
            dataRequest = request,
            fieldBindings = Seq(
              FieldBinding
                .newBuilder()
                .setTupleFieldName("field")
                .setStoreFieldName("family:column1")
                .build()),
            kvstores = Seq()),
          scoreEnvironment = new ScoreEnvironment(
            outputColumn = "family:column2",
            kvstores = Seq()))
        doAndClose(new FileWriter(modelEnvFile)) { writer =>
          writer.write(modelEnvironment.toJson())
        }

        assert(0 === ExtractScoreJobTool.main(Array("--model-def=" + modelDefFile.getAbsolutePath,
            "--model-env=" + modelEnvFile.getAbsolutePath)))
        doAndClose(table.openTableReader()) { reader: KijiTableReader =>
          val v1 = reader
            .get(table.getEntityId("row1"), KijiDataRequest.create("family", "column2"))
            .getMostRecentValue("family", "column2")
            .toString
          val v2 = reader
            .get(table.getEntityId("row2"), KijiDataRequest.create("family", "column2"))
            .getMostRecentValue("family", "column2")
            .toString

          assert("FOOFOO" === v1)
          assert("BARBAR" === v2)
        }
      }

    } finally {
      tmpDir.delete()
      modelDefFile.delete()
      modelEnvFile.delete()
    }
  }

  test("ExtractScoreJobTool validates the --model-def flag.") {
    val thrown = intercept[IllegalArgumentException] {
      ExtractScoreJobTool.main(Array("--model-env=/some/path"))
    }
    assert("requirement failed: Specify the Model Definition to use with" +
      " --model-def=/path/to/model-def.json" === thrown.getMessage)
  }

  test("ExtractScoreJobTool validates the --model-env flag.") {
    val thrown = intercept[IllegalArgumentException] {
      ExtractScoreJobTool.main(Array("--model-def=/some/path"))
    }
    assert("requirement failed: Specify the Model Environment to use with" +
      " --model-env=/path/to/model-env.json" === thrown.getMessage)
  }
}
