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

package org.kiji.modeling.framework

import org.apache.hadoop.fs.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.modeling.Extractor
import org.kiji.modeling.KeyValueStore
import org.kiji.modeling.ScoreProducerJobBuilder
import org.kiji.modeling.Scorer
import org.kiji.modeling.config.ExpressColumnRequest
import org.kiji.modeling.config.ExpressDataRequest
import org.kiji.modeling.config.FieldBinding
import org.kiji.modeling.config.KeyValueStoreSpec
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.ScoreEnvironment
import org.kiji.modeling.impl.KeyValueStoreImplSuite
import org.kiji.modeling.lib.FirstValueExtractor
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class ScoreProducerSuite
    extends KijiSuite {
  test("An extract-score produce job can be run over a table.") {
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

      // Update configuration object with appropriately serialized ModelDefinition/ModelEnvironment
      // JSON.
      val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
          new ExpressColumnRequest("family:column1", 1, None) :: Nil)
      val sideDataPath: Path = KeyValueStoreImplSuite.generateAvroKVRecordKeyValueStore()
      val modelDefinition: ModelDefinition = ModelDefinition(
          name = "test-model-definition",
          version = "1.0",
          scoreExtractorClass = Some(classOf[ScoreProducerSuite.DoublingExtractor]),
          scorerClass = Some(classOf[ScoreProducerSuite.UpperCaseScorer]))
      val modelEnvironment: ModelEnvironment = ModelEnvironment(
          name = "test-model-environment",
          version = "1.0",
          prepareEnvironment = None,
          trainEnvironment = None,
        scoreEnvironment = Some(ScoreEnvironment(
              KijiInputSpec(
                  uri.toString,
                  dataRequest = request,
                  fieldBindings = Seq(
                      FieldBinding(tupleFieldName = "field", storeFieldName = "family:column1"))),
              KijiSingleColumnOutputSpec(uri.toString, "family:column2"),
              keyValueStoreSpecs = Seq(
                  KeyValueStoreSpec(
                      storeType = "AVRO_KV",
                      name = "side_data",
                      properties = Map(
                          "path" -> sideDataPath.toString,
                          // The Distributed Cache is not supported when using LocalJobRunner in
                          // Hadoop <= 0.21.0.
                          // See https://issues.apache.org/jira/browse/MAPREDUCE-476 for more
                          // information.
                          "use_dcache" -> "false"))))))

      // Build the produce job.
      val produceJob = ScoreProducerJobBuilder.buildJob(
          model = modelDefinition,
          environment = modelEnvironment)

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

        assert("FOOFOOONE" === v1)
        assert("BARBARONE" === v2)
      }
    }
    kiji.release()
  }

  test("An extract-score produce job using multiple fields can be run over a table.") {
    val testLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

    val kiji: Kiji = new InstanceBuilder("default")
        .withTable(testLayout.getName, testLayout)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column1").withValue("foo")
                    .withQualifier("column2").withValue("baz")
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("column1").withValue("bar")
                    .withQualifier("column2").withValue("foo")
        .build()

    doAndRelease(kiji.openTable(testLayout.getName)) { table: KijiTable =>
      val uri: KijiURI = table.getURI

      // Update configuration object with appropriately serialized ModelDefinition/ModelEnvironment
      // JSON.
      val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
          new ExpressColumnRequest("family:column1", 1, None) ::
          new ExpressColumnRequest("family:column2", 1, None) :: Nil)
      val modelDefinition: ModelDefinition = ModelDefinition(
          name = "test-model-definition",
          version = "1.0",
          scoreExtractorClass = Some(classOf[ScoreProducerSuite.TwoArgDoublingExtractor]),
          scorerClass = Some(classOf[ScoreProducerSuite.TwoArgUpperCaseScorer]))
      val modelEnvironment: ModelEnvironment = ModelEnvironment(
          name = "test-model-environment",
          version = "1.0",
          prepareEnvironment = None,
          trainEnvironment = None,
          scoreEnvironment = Some(ScoreEnvironment(
              KijiInputSpec(
                  uri.toString,
                  dataRequest = request,
                  fieldBindings = Seq(
                      FieldBinding(tupleFieldName = "i1", storeFieldName = "family:column1"),
                      FieldBinding(tupleFieldName = "i2", storeFieldName = "family:column2"))),
              KijiSingleColumnOutputSpec(uri.toString, "family:column2"),
              keyValueStoreSpecs = Seq())))

      // Build the produce job.
      val produceJob = ScoreProducerJobBuilder.buildJob(
          model = modelDefinition,
          environment = modelEnvironment)

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
    kiji.release()
  }

  test("An extract-score produce job using an entityId field can be run over a table.") {
    val testLayout: KijiTableLayout = layout(KijiTableLayouts.FORMATTED_RKF)

    val kiji: Kiji = new InstanceBuilder("default")
        .withTable(testLayout.getName, testLayout)
            .withRow("cmpA1", "cmpB1", "cmpC1", 1: java.lang.Integer, 1L: java.lang.Long)
                .withFamily("family")
                    .withQualifier("column")
                        .withValue("foo")
                        .withValue("baz")
            .withRow("cmpA2", "cmpB2", "cmpC2", 2: java.lang.Integer, 2L: java.lang.Long)
                .withFamily("family")
                    .withQualifier("column")
                        .withValue("bar")
                        .withValue("foo")
        .build()

    doAndRelease(kiji.openTable(testLayout.getName)) { table: KijiTable =>
      val uri: KijiURI = table.getURI

      // Update configuration object with appropriately serialized ModelDefinition/ModelEnvironment
      // JSON.
      val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
          new ExpressColumnRequest("family:column", 1, None) :: Nil)
      val modelDefinition: ModelDefinition = ModelDefinition(
          name = "test-model-definition",
          version = "1.0",
          scoreExtractorClass = Some(classOf[ScoreProducerSuite.EntityIdExtractor]),
          scorerClass = Some(classOf[ScoreProducerSuite.TwoArgUpperCaseScorer]))
      val modelEnvironment: ModelEnvironment = ModelEnvironment(
          name = "test-model-environment",
          version = "1.0",
          prepareEnvironment = None,
          trainEnvironment = None,
          scoreEnvironment = Some(ScoreEnvironment(
              KijiInputSpec(
                  uri.toString,
                  dataRequest = request,
                  fieldBindings = Seq(
                      FieldBinding(tupleFieldName = "i1", storeFieldName = "family:column"))),
              KijiSingleColumnOutputSpec(uri.toString, "family:column"),
              keyValueStoreSpecs = Seq())))

      // Build the produce job.
      val produceJob = ScoreProducerJobBuilder.buildJob(
          model = modelDefinition,
          environment = modelEnvironment)

      // Verify that everything went as expected.
      assert(produceJob.run())
      doAndClose(table.openTableReader()) { reader: KijiTableReader =>
        val eid1 = table.getEntityId(
            "cmpA1",
            "cmpB1",
            "cmpC1",
            1: java.lang.Integer,
            1L: java.lang.Long)
        val eid2 = table.getEntityId(
            "cmpA2",
            "cmpB2",
            "cmpC2",
            2: java.lang.Integer,
            2L: java.lang.Long)
        val v1 = reader
            .get(eid1, KijiDataRequest.create("family", "column"))
            .getMostRecentValue("family", "column")
            .toString
        val v2 = reader
            .get(eid2, KijiDataRequest.create("family", "column"))
            .getMostRecentValue("family", "column")
            .toString

        assert("CMPA1CMPB1" === v1)
        assert("CMPA2CMPB2" === v2)
      }
    }
    kiji.release()
  }

  test("An extract-score produce job using SelectorExtractor can be run over a table.") {
    val testLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

    val kiji: Kiji = new InstanceBuilder("default")
        .withTable(testLayout.getName, testLayout)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column1").withValue(1L, "foo1")
                    .withQualifier("column1").withValue(2L, "foo2")
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("column1").withValue(1L, "bar1")
                    .withQualifier("column1").withValue(2L, "bar2")
        .build()

    doAndRelease(kiji.openTable(testLayout.getName)) { table: KijiTable =>
      val uri: KijiURI = table.getURI

      // Update configuration object with appropriately serialized ModelDefinition/ModelEnvironment
      // JSON.
      val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
          new ExpressColumnRequest("family:column1", 1, None) :: Nil)
      val sideDataPath: Path = KeyValueStoreImplSuite.generateAvroKVRecordKeyValueStore()
      val modelDefinition: ModelDefinition = ModelDefinition(
          name = "test-model-definition",
          version = "1.0",
          scoreExtractorClass = Some(classOf[FirstValueExtractor]),
          scorerClass = Some(classOf[ScoreProducerSuite.UpperCaseScorer]))
      val modelEnvironment: ModelEnvironment = ModelEnvironment(
          name = "test-model-environment",
          version = "1.0",
          prepareEnvironment = None,
          trainEnvironment = None,
          scoreEnvironment = Some(ScoreEnvironment(
              KijiInputSpec(
                  uri.toString,
                  request,
                  Seq(
                      FieldBinding(tupleFieldName = "feature", storeFieldName = "family:column1"))
              ),
              KijiSingleColumnOutputSpec(uri.toString, "family:column2"),
              keyValueStoreSpecs = Seq())))

      // Build the produce job.
      val produceJob = ScoreProducerJobBuilder.buildJob(
          model = modelDefinition,
          environment = modelEnvironment)

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

        assert("FOO2" === v1)
        assert("BAR2" === v2)
      }
    }
    kiji.release()
  }

  test("Extractor is not required") {
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

      // Update configuration object with appropriately serialized ModelDefinition/ModelEnvironment
      // JSON.
      val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
        new ExpressColumnRequest("family:column1", 1, None) :: Nil)
      val sideDataPath: Path = KeyValueStoreImplSuite.generateAvroKVRecordKeyValueStore()
      val modelDefinition: ModelDefinition = ModelDefinition(
        name = "test-model-definition",
        version = "1.0",
        scoreExtractorClass = None,
        scorerClass = Some(classOf[ScoreProducerSuite.KijiSliceScorer]))
      val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "test-model-environment",
        version = "1.0",
        prepareEnvironment = None,
        trainEnvironment = None,
        scoreEnvironment = Some(ScoreEnvironment(
          KijiInputSpec(
            uri.toString,
            dataRequest = request,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "field", storeFieldName = "family:column1"))),
          KijiSingleColumnOutputSpec(uri.toString, "family:column2"),
          keyValueStoreSpecs = Seq(
            KeyValueStoreSpec(
              storeType = "AVRO_KV",
              name = "side_data",
              properties = Map(
                "path" -> sideDataPath.toString,
                // The Distributed Cache is not supported when using LocalJobRunner in
                // Hadoop <= 0.21.0.
                // See https://issues.apache.org/jira/browse/MAPREDUCE-476 for more
                // information.
                "use_dcache" -> "false"))))))

      // Build the produce job.
      val produceJob = ScoreProducerJobBuilder.buildJob(
        model = modelDefinition,
        environment = modelEnvironment)

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

        assert("foo" === v1)
        assert("bar" === v2)
      }
    }
    kiji.release()
  }
}

object ScoreProducerSuite {
  class DoublingExtractor extends Extractor {
    override val extractFn = extract('field -> 'feature) { field: KijiSlice[String] =>
      val str: String = field.getFirstValue
      val sideData: KeyValueStore[Int, String] = keyValueStore("side_data")

      str + str + sideData(1)
    }
  }

  class UpperCaseScorer extends Scorer {
    override val scoreFn = score('feature) { feature: String =>
      feature.toUpperCase
    }
  }

  class KijiSliceScorer extends Scorer {
    override val scoreFn = score('field) { field: KijiSlice[String] =>
      field.getFirstValue
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

  class EntityIdExtractor extends Extractor {
    override val extractFn = extract('entityId -> ('x1, 'x2)) { input: EntityId =>
      (input(0), input(1))
    }
  }
}
