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

import com.twitter.scalding.Hdfs
import com.twitter.scalding.Source
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.All
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.util.Resources.doAndClose
import org.kiji.express.flow.util.Resources.withKijiTable
import org.kiji.modeling.Preparer
import org.kiji.modeling.config.FieldBinding
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiOutputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.PrepareEnvironment
import org.kiji.modeling.framework.ModelExecutor
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

/**
 * This tests the flow of a very basic iterative preparer.
 */
@RunWith(classOf[JUnitRunner])
class IterativePreparerSuite extends KijiSuite {
  test("An iterative preparer works properly") {
    val testLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS).getDesc
    testLayoutDesc.setName("input_table")

    val kiji: Kiji = new InstanceBuilder("default")
        .withTable(testLayoutDesc)
        .withRow("row1")
        .withFamily("family")
        .withQualifier("column1").withValue("foo")
        .withRow("row2")
        .withFamily("family")
        .withQualifier("column1").withValue("bar")
        .withRow("row3")
        .withFamily("family")
        .withQualifier("column1").withValue("baz")
        .build()

    val modelDefinition: ModelDefinition = ModelDefinition(
        name = "iterative-prepare-model-def",
        version = "1.0",
        preparerClass = Some(classOf[IterativePreparerSuite.IterativePreparer]))

    withKijiTable(kiji, "input_table") { table: KijiTable =>
      val tableUri: KijiURI = table.getURI

      val fieldB1 = Seq(FieldBinding(tupleFieldName = "word", storeFieldName = "family:column1"))
      val fieldB2 = Seq(FieldBinding(tupleFieldName = "word", storeFieldName = "family:column2"))

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
          name = "prepare-model-environment",
          version = "1.0",
          prepareEnvironment = Some(PrepareEnvironment(
              inputSpec = Map(
                  "input1" -> KijiInputSpec(
                      tableUri.toString,
                      timeRange=All,
                      columnsToFields =
                          Map(QualifiedColumnInputSpec("family", "column1") -> 'word)),
                  "input2" -> KijiInputSpec(
                      tableUri.toString,
                      timeRange=All,
                      columnsToFields =
                          Map(QualifiedColumnInputSpec("family", "column2") -> 'word))
              ),
              outputSpec = Map(
                  "output" -> KijiOutputSpec(
                      tableUri = tableUri.toString,
                      fieldsToColumns = Map('word ->
                          QualifiedColumnOutputSpec.builder
                              .withColumn("family", "column2")
                              .build))
              ),
              keyValueStoreSpecs = Seq()
          )),
          trainEnvironment = None,
          scoreEnvironment = None
      )

      // Build the produce job.
      val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)

      // Hack to set the mode correctly. Scalding sets the mode in JobTest
      // which creates a problem for running the prepare/train phases, which run
      // their own jobs. This makes the test below run in HadoopTest mode instead
      // of Hadoop mode whenever it is run after another test that uses JobTest.
      // Remove this after the bug in Scalding is fixed.
      com.twitter.scalding.Mode.mode = Hdfs(false, HBaseConfiguration.create())

      // Verify that everything went as expected.
      assert(modelExecutor.runPreparer())

      doAndClose(table.openTableReader()) { reader: KijiTableReader =>
        val v1 = reader
            .get(table.getEntityId("row1"), KijiDataRequest.create("family", "column2"))
            .getMostRecentValue("family", "column2")
            .toString

        assert("foofoofoofoo" === v1)
      }
    }
    kiji.release()
  }
}

/**
 * Companion object for [[org.kiji.modeling.lib.IterativePreparerSuite]].
 */
object IterativePreparerSuite {
  /**
   * The IterativePreparer runs a contrived iterative job. The job reads a word
   * from the provided table/column (family:column1) and concatenates it with itself.
   * It writes the output to family:column2. In the next iteration, we make the output
   * column as the input column, so that the resulting word is now quadrupled.
   * Thus foo in family:column1 should result in foofoofoofoo in family:column2 at the end.
   */
  class IterativePreparer extends Preparer {
    class IterativeJob(input: Source, output: Source) extends PreparerJob {
      input
          .read
          .map('word -> 'cleanWord) { words: Seq[FlowCell[CharSequence]] =>
             words
                 .head
                 .datum
                 .toString
                 .toLowerCase
          }
          .map('cleanWord -> 'doubleWord) { cleanWord: String =>
            cleanWord.concat(cleanWord)
          }
          .project('entityId, 'doubleWord)
          .rename('doubleWord -> 'word)
          .write(output)
    }

    override def prepare(input: Map[String, Source], output: Map[String, Source]): Boolean = {
      new IterativeJob(input("input1"), output("output")).run
      new IterativeJob(input("input2"), output("output")).run
      true
    }
  }
}
