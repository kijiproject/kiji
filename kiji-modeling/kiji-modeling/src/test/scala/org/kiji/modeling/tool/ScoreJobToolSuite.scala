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
import java.io.FileWriter

import com.google.common.io.Files
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.All
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.modeling.Extractor
import org.kiji.modeling.Scorer
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.ScoreEnvironment
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class ScoreJobToolSuite extends KijiSuite {
  test("ScoreJobTool can run a job.") {
    val tmpDir: File = Files.createTempDir()
    val modelDefFile: File = new File(tmpDir, "model-def.json")
    val modelEnvFile: File = new File(tmpDir, "model-env.json")

    try {
      // Setup the test environment.
      val testLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
      val kiji: Kiji = new InstanceBuilder("default")
          .withTable(testLayout.getName, testLayout)
              .withRow("row1")
                  .withFamily("family")
                      .withQualifier("column1").withValue("foo")
              .withRow("row2")
                  .withFamily("family")
                      .withQualifier("column1").withValue("bar")
          .build()

      doAndRelease(kiji.openTable(testLayout.getName)) { table: KijiTable =>
        val uri: KijiURI = table.getURI

        // Create a model definition and environment.
        val modelDefinition: ModelDefinition = ModelDefinition(
            name = "test-model-definition",
            version = "1.0",
            scoreExtractorClass = Some(classOf[ScoreJobToolSuite.DoublingExtractor]),
            scorerClass = Some(classOf[ScoreJobToolSuite.UpperCaseScorer]))
        val modelEnvironment: ModelEnvironment = ModelEnvironment(
            name = "test-model-environment",
            version = "1.0",
            prepareEnvironment = None,
            trainEnvironment = None,
            scoreEnvironment = Some(ScoreEnvironment(
                KijiInputSpec(
                    uri.toString,
                    timeRange=All,
                    columnsToFields =
                        Map(QualifiedColumnInputSpec.builder
                            .withColumn("family", "column1")
                            .build -> 'field)
                ),
                KijiSingleColumnOutputSpec(
                    uri.toString,
                  QualifiedColumnOutputSpec.builder
                      .withColumn("family", "column2")
                      .build
                ),
                keyValueStoreSpecs = Seq())))

        // Write the created model definition and environment to disk.
        doAndClose(new FileWriter(modelDefFile)) { writer =>
          writer.write(modelDefinition.toJson)
        }
        doAndClose(new FileWriter(modelEnvFile)) { writer =>
          writer.write(modelEnvironment.toJson)
        }

        // Run the tool.
        ScoreJobTool.main(Array(
            "--model-def=" + modelDefFile.getAbsolutePath,
            "--model-env=" + modelEnvFile.getAbsolutePath))

        // Validate the results of running the model.
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
      // Cleanup.
      tmpDir.delete()
      modelDefFile.delete()
      modelEnvFile.delete()
    }
  }

  test("ScoreJobTool validates the --model-def flag.") {
    val thrown = intercept[IllegalArgumentException] {
      ScoreJobTool.main(Array("--model-env=/some/path"))
    }
    assert("requirement failed: Specify the Model Definition to use with" +
      " --model-def=/path/to/model-def.json" === thrown.getMessage)
  }

  test("ScoreJobTool validates the --model-env flag.") {
    val thrown = intercept[IllegalArgumentException] {
      ScoreJobTool.main(Array("--model-def=/some/path"))
    }
    assert("requirement failed: Specify the Model Environment to use with" +
      " --model-env=/path/to/model-env.json" === thrown.getMessage)
  }
}

object ScoreJobToolSuite {
  class DoublingExtractor extends Extractor {
    override val extractFn = extract('field -> 'feature) { field: Seq[FlowCell[CharSequence]] =>
      val str: String = field.head.datum.toString
      str + str
    }
  }

  class UpperCaseScorer extends Scorer {
    override val scoreFn = score('feature) { feature: String =>
      feature.toUpperCase
    }
  }
}
