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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.Resources.doAndClose
import org.kiji.express.Resources.doAndRelease
import org.kiji.express.avro.FieldBinding
import org.kiji.mapreduce.KijiMapReduceJob
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput
import org.kiji.mapreduce.produce.KijiProduceJobBuilder
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

class ExtractScoreProducerSuite
    extends KijiSuite {
  test("A produce job using ExtractScoreProducer can be run over a table.") {
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

      // Update configuration object with appropriately serialized ModelSpec/RunSpec JSON.
      val request: KijiDataRequest = KijiDataRequest.create("family", "column1")
      val modelSpec: ModelSpec = new ModelSpec(
          name = "test-model-spec",
          version = "1.0",
          extractorClass = classOf[ExtractScoreProducerSuite.DoublingExtractor],
          scorerClass = classOf[ExtractScoreProducerSuite.UpperCaseScorer])
      val runSpec: RunSpec = new RunSpec(
          name = "test-run-spec",
          version = "1.0",
          modelTableUri = uri.toString,
          extractRunSpec = new ExtractRunSpec(
              dataRequest = request,
              fieldBindings = Seq(
                  FieldBinding
                      .newBuilder()
                      .setTupleFieldName("field")
                      .setStoreFieldName("family:column1")
                      .build()),
              kvstores = Seq()),
          scoreRunSpec = new ScoreRunSpec(
              outputColumn = "family:column2",
              kvstores = Seq()))
      val conf: Configuration = HBaseConfiguration.create()
      conf.set(ExtractScoreProducer.modelSpecConfKey, modelSpec.toJson())
      conf.set(ExtractScoreProducer.runSpecConfKey, runSpec.toJson())

      // Build the produce job.
      val produceJob: KijiMapReduceJob = KijiProduceJobBuilder.create()
          .withConf(conf)
          .withInputTable(uri)
          .withProducer(classOf[ExtractScoreProducer])
          .withOutput(new DirectKijiTableMapReduceJobOutput(uri))
          .build()

      // Verify that everything went as expected.
      assert(produceJob.run())
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
  }

  test("A produce job using ExtractScoreProducer with multiple args can be run over a table.") {
    val testLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

    val kiji: Kiji = new InstanceBuilder("default")
        .withTable(testLayout.getName(), testLayout)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column1").withValue("foo")
                    .withQualifier("column2").withValue("baz")
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("column1").withValue("bar")
                    .withQualifier("column2").withValue("foo")
        .build()

    doAndRelease(kiji.openTable(testLayout.getName())) { table: KijiTable =>
      val uri: KijiURI = table.getURI()

      // Update configuration object with appropriately serialized ModelSpec/RunSpec JSON.
      val request: KijiDataRequest = {
        val builder = KijiDataRequest.builder()
        builder.newColumnsDef().add("family", "column1")
        builder.newColumnsDef().add("family", "column2")
        builder.build()
      }
      val modelSpec: ModelSpec = new ModelSpec(
          name = "test-model-spec",
          version = "1.0",
          extractorClass = classOf[ExtractScoreProducerSuite.TwoArgDoublingExtractor],
          scorerClass = classOf[ExtractScoreProducerSuite.TwoArgUpperCaseScorer])
      val runSpec: RunSpec = new RunSpec(
          name = "test-run-spec",
          version = "1.0",
          modelTableUri = uri.toString,
          extractRunSpec = new ExtractRunSpec(
              dataRequest = request,
              fieldBindings = Seq(
                  FieldBinding
                      .newBuilder()
                      .setTupleFieldName("i1")
                      .setStoreFieldName("family:column1")
                      .build(),
                  FieldBinding
                      .newBuilder()
                      .setTupleFieldName("i2")
                      .setStoreFieldName("family:column2")
                      .build()),
              kvstores = Seq()),
          scoreRunSpec = new ScoreRunSpec(
              outputColumn = "family:column2",
              kvstores = Seq()))
      val conf: Configuration = kiji.getConf()
      conf.set(ExtractScoreProducer.modelSpecConfKey, modelSpec.toJson())
      conf.set(ExtractScoreProducer.runSpecConfKey, runSpec.toJson())

      // Build the produce job.
      val produceJob: KijiMapReduceJob = KijiProduceJobBuilder.create()
          .withConf(conf)
          .withInputTable(uri)
          .withProducer(classOf[ExtractScoreProducer])
          .withOutput(new DirectKijiTableMapReduceJobOutput(uri))
          .build()

      // Verify that everything went as expected.
      assert(produceJob.run())
      doAndClose(table.openTableReader()) { reader: KijiTableReader =>
        val v1 = reader
            .get(table.getEntityId("row1"), KijiDataRequest.create("family", "column2"))
            .getMostRecentValue("family", "column2")
            .toString
        val v2 = reader
            .get(table.getEntityId("row2"), KijiDataRequest.create("family", "column2"))
            .getMostRecentValue("family", "column2")
            .toString

        assert("FOOFOOBAZBAZ" === v1)
        assert("BARBARFOOFOO" === v2)
      }
    }
  }
}

object ExtractScoreProducerSuite {
  class DoublingExtractor extends Extractor {
    override val extractFn = extract('field -> 'feature) { field: KijiSlice[String] =>
      val str: String = field.getFirstValue
      str + str
    }
  }

  class UpperCaseScorer extends Scorer {
    override val scoreFn = score('feature) { feature: String =>
      feature.toUpperCase
    }
  }

  class TwoArgDoublingExtractor extends Extractor {
    override val extractFn =
        extract(('i1, 'i2) -> ('x1, 'x2)) { input: (KijiSlice[String], KijiSlice[String]) =>
          val (i1, i2) = input

          (i1.getFirstValue + i1.getFirstValue, i2.getFirstValue + i2.getFirstValue)
        }
  }

  class TwoArgUpperCaseScorer extends Scorer {
    override val scoreFn = score(('x1, 'x2)) { features: (String, String) =>
      val (x1, x2) = features

      x1.toUpperCase + x2.toUpperCase
    }
  }
}
