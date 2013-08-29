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

package org.kiji.express.modeling.framework

import scala.Some

import com.twitter.scalding.Hdfs
import com.twitter.scalding.Source
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.modeling.Preparer
import org.kiji.express.modeling.Trainer
import org.kiji.express.modeling.config.ExpressColumnRequest
import org.kiji.express.modeling.config.ExpressDataRequest
import org.kiji.express.modeling.config.FieldBinding
import org.kiji.express.modeling.config.KijiInputSpec
import org.kiji.express.modeling.config.KijiOutputSpec
import org.kiji.express.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.express.modeling.config.KVStore
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.config.PrepareEnvironment
import org.kiji.express.modeling.config.ScoreEnvironment
import org.kiji.express.modeling.config.TrainEnvironment
import org.kiji.express.modeling.impl.KeyValueStoreImplSuite
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

class ModelExecutorSuite extends KijiSuite {
  test("A ModelExecutor is properly created") {
    val modelDef: ModelDefinition = ModelDefinition(
        name = "test-model-definition",
        version = "1.0",
        preparer = Some(classOf[ModelExecutorSuite.PrepareWordCounter]),
        trainer = Some(classOf[ModelExecutorSuite.TrainWordCounter]),
        scoreExtractor = Some(classOf[ScoreProducerSuite.DoublingExtractor]),
        scorer = Some(classOf[ScoreProducerSuite.UpperCaseScorer]))
    val modelEnv = ModelEnvironment(
        "myname",
        "1.0.0",
        None,
        None,
        None)
    ModelExecutor(modelDef, modelEnv)
  }

  test("An extract-score job can be run over a table.") {
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

      // Update configuration object with appropriately serialized ModelDefinition/ModelEnvironment
      // JSON.
      val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
        new ExpressColumnRequest("family:column1", 1, None) :: Nil)
      val sideDataPath: Path = KeyValueStoreImplSuite.generateAvroKVRecordKeyValueStore()
      val modelDefinition: ModelDefinition = ModelDefinition(
        name = "test-model-definition",
        version = "1.0",
        scoreExtractor = Some(classOf[ScoreProducerSuite.DoublingExtractor]),
        scorer = Some(classOf[ScoreProducerSuite.UpperCaseScorer]))
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
          kvstores = Seq(
            KVStore(
              storeType = "AVRO_KV",
              name = "side_data",
              properties = Map(
                "path" -> sideDataPath.toString(),
                // The Distributed Cache is not supported when using LocalJobRunner in
                // Hadoop <= 0.21.0.
                // See https://issues.apache.org/jira/browse/MAPREDUCE-476 for more
                // information.
                "use_dcache" -> "false"))))))

      // Build the produce job.
      val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)

      // Verify that everything went as expected.
      assert(modelExecutor.runScorer())
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

  test("A prepare job can be run over a table") {
    val testLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS).getDesc
    testLayoutDesc.setName("input_table")

    val outputLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE).getDesc
    outputLayoutDesc.setName("output_table")

    val kiji: Kiji = new InstanceBuilder("default")
        .withTable(outputLayoutDesc)
        .withTable(testLayoutDesc)
        .withRow("row1")
        .withFamily("family")
        .withQualifier("column1").withValue("foo")
        .withRow("row2")
        .withFamily("family")
        .withQualifier("column1").withValue("bar")
        .withRow("row3")
        .withFamily("family")
        .withQualifier("column1").withValue("bar")
        .build()

    val modelDefinition: ModelDefinition = ModelDefinition(
        name = "prepare-model-definition",
        version = "1.0",
        preparer = Some(classOf[ModelExecutorSuite.PrepareWordCounter]))

    val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
        new ExpressColumnRequest("family:column1", 1, None) :: Nil)

    val inputUri: KijiURI = doAndRelease(kiji.openTable("input_table")) { table: KijiTable =>
      table.getURI()}

    doAndRelease(kiji.openTable("output_table")) { table: KijiTable =>
      val outputUri: KijiURI = table.getURI()

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "prepare-model-environment",
        version = "1.0",
        prepareEnvironment = Some(PrepareEnvironment(
            inputConfig = KijiInputSpec(
                inputUri.toString,
                dataRequest = request,
                fieldBindings = Seq(
                    FieldBinding(tupleFieldName = "word", storeFieldName = "family:column1"))
            ),
            outputConfig = KijiOutputSpec(
                tableUri = outputUri.toString,
                fieldBindings = Seq(
                    FieldBinding(tupleFieldName = "size", storeFieldName = "family:column"))
            ),
            kvstores = Seq()
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
          .get(table.getEntityId("foo"), KijiDataRequest.create("family", "column"))
          .getMostRecentValue("family", "column")
          .toString
        val v2 = reader
          .get(table.getEntityId("bar"), KijiDataRequest.create("family", "column"))
          .getMostRecentValue("family", "column")
          .toString

        assert("1" === v1)
        assert("2" === v2)
      }
    }
    kiji.release()

  }

  test("A train job can be run over a table") {
    val testLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS).getDesc
    testLayoutDesc.setName("input_table")

    val outputLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE).getDesc
    outputLayoutDesc.setName("output_table")

    val kiji: Kiji = new InstanceBuilder("default")
      .withTable(outputLayoutDesc)
      .withTable(testLayoutDesc)
      .withRow("row1")
      .withFamily("family")
      .withQualifier("column1").withValue("foo")
      .withRow("row2")
      .withFamily("family")
      .withQualifier("column1").withValue("bar")
      .withRow("row3")
      .withFamily("family")
      .withQualifier("column1").withValue("bar")
      .build()

    val modelDefinition: ModelDefinition = ModelDefinition(
      name = "prepare-model-definition",
      version = "1.0",
      trainer = Some(classOf[ModelExecutorSuite.TrainWordCounter]))

    val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
      new ExpressColumnRequest("family:column1", 1, None) :: Nil)

    val inputUri: KijiURI = doAndRelease(kiji.openTable("input_table")) { table: KijiTable =>
      table.getURI()}

    doAndRelease(kiji.openTable("output_table")) { table: KijiTable =>
      val outputUri: KijiURI = table.getURI()

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "prepare-model-environment",
        version = "1.0",
        trainEnvironment = Some(TrainEnvironment(
          inputConfig = KijiInputSpec(
            inputUri.toString,
            dataRequest = request,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "word", storeFieldName = "family:column1"))
          ),
          outputConfig = KijiOutputSpec(
            tableUri = outputUri.toString,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "size", storeFieldName = "family:column"))
          ),
          kvstores = Seq()
        )),
        prepareEnvironment = None,
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
      assert(modelExecutor.runTrainer())

      doAndClose(table.openTableReader()) { reader: KijiTableReader =>
        val v1 = reader
          .get(table.getEntityId("foo"), KijiDataRequest.create("family", "column"))
          .getMostRecentValue("family", "column")
          .toString
        val v2 = reader
          .get(table.getEntityId("bar"), KijiDataRequest.create("family", "column"))
          .getMostRecentValue("family", "column")
          .toString

        assert("1" === v1)
        assert("2" === v2)
      }
    }
    kiji.release()

  }

  test("A prepare-train-score job can be run over a table") {
    //  The prepare phase will count the number of occurences in of values in family:column1
    //  column of the input_table and write this to the output_table.
    //  The train phase will read the output_table and calculate an average of the counts, and
    //  write this value to the output_table at a row with entityId "AVERAGE".
    //  The score phase uses a doubling extractor and an upper case scorer. It takes as input
    //  the average calculated by the trainer. It writes the output to the output_table at a row
    //  with entityId "FINAL".

    val testLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS).getDesc
    testLayoutDesc.setName("input_table")

    val prepareOutputLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE).getDesc
    prepareOutputLayoutDesc.setName("prepare_output_table")

    val trainOutputLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS).getDesc
    trainOutputLayoutDesc.setName("train_output_table")

    val kiji: Kiji = new InstanceBuilder("default")
      .withTable(prepareOutputLayoutDesc)
      .withTable(trainOutputLayoutDesc)
      .withTable(testLayoutDesc)
      .withRow("row1")
      .withFamily("family")
      .withQualifier("column1").withValue("foo")
      .withRow("row2")
      .withFamily("family")
      .withQualifier("column1").withValue("bar")
      .withRow("row3")
      .withFamily("family")
      .withQualifier("column1").withValue("bar")
      .build()

    val modelDefinition: ModelDefinition = ModelDefinition(
      name = "prepare-model-definition",
      version = "1.0",
      preparer = Some(classOf[ModelExecutorSuite.PrepareWordCounter]),
      trainer = Some(classOf[ModelExecutorSuite.AverageTrainer]),
      scoreExtractor = Some(classOf[ScoreProducerSuite.DoublingExtractor]),
      scorer = Some(classOf[ScoreProducerSuite.UpperCaseScorer])
    )

    val prepareRequest: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
      new ExpressColumnRequest("family:column1", 1, None) :: Nil)

    val trainRequest: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
      new ExpressColumnRequest("family:column", 1, None) :: Nil)

    val scoreRequest: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
      new ExpressColumnRequest("family:column1", 1, None) :: Nil)

    val sideDataPath: Path = KeyValueStoreImplSuite.generateAvroKVRecordKeyValueStore()

    val inputUri: KijiURI = doAndRelease(kiji.openTable("input_table")) { table: KijiTable =>
      table.getURI()}

    val prepareOutputUri: KijiURI = doAndRelease(kiji.openTable("prepare_output_table")) {
      table: KijiTable => table.getURI()}

    doAndRelease(kiji.openTable("train_output_table")) { table: KijiTable =>
      val trainOutputUri: KijiURI = table.getURI()

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "prepare-model-environment",
        version = "1.0",
        prepareEnvironment = Some(PrepareEnvironment(
          inputConfig = KijiInputSpec(
            inputUri.toString,
            dataRequest = prepareRequest,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "word", storeFieldName = "family:column1"))
          ),
          outputConfig = KijiOutputSpec(
            tableUri = prepareOutputUri.toString,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "size", storeFieldName = "family:column"))
          ),
          kvstores = Seq()
        )),
        trainEnvironment = Some(TrainEnvironment(
          inputConfig = KijiInputSpec(
            prepareOutputUri.toString,
            dataRequest = trainRequest,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "word", storeFieldName = "family:column"))
          ),
          outputConfig = KijiOutputSpec(
            trainOutputUri.toString,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "avgString", storeFieldName = "family:column1"))
          ),
          kvstores = Seq()
        )),
        scoreEnvironment = Some(ScoreEnvironment(
          inputConfig = KijiInputSpec(
            trainOutputUri.toString,
            dataRequest = scoreRequest,
            fieldBindings = Seq(
              FieldBinding(tupleFieldName = "field", storeFieldName = "family:column1"))
          ),
          outputConfig = KijiSingleColumnOutputSpec(
            trainOutputUri.toString,
            "family:column2"
          ),
          kvstores = Seq(
              KVStore(
                  storeType = "AVRO_KV",
                  name = "side_data",
                  properties = Map(
                      "path" -> sideDataPath.toString(),
                      // The Distributed Cache is not supported when using LocalJobRunner in
                      // Hadoop <= 0.21.0.
                      // See https://issues.apache.org/jira/browse/MAPREDUCE-476 for more
                      // information.
                      "use_dcache" -> "false")))
        ))
      )

      // Build the model executor.
      val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)

      // Hack to set the mode correctly. Scalding sets the mode in JobTest
      // which creates a problem for running the prepare/train phases, which run
      // their own jobs. This makes the test below run in HadoopTest mode instead
      // of Hadoop mode whenever it is run after another test that uses JobTest.
      // Remove this after the bug in Scalding is fixed.
      com.twitter.scalding.Mode.mode = Hdfs(false, HBaseConfiguration.create())

      // Verify that everything went as expected.
      assert(modelExecutor.run())

      doAndClose(table.openTableReader()) { reader: KijiTableReader =>
        val v1 = reader
          .get(table.getEntityId("AVERAGE"), KijiDataRequest.create("family", "column2"))
          .getMostRecentValue("family", "column2")
          .toString

        assert("1.5 UNITS1.5 UNITSONE" === v1)
      }
    }
    kiji.release()

  }
}

object ModelExecutorSuite {
  class PrepareWordCounter extends Preparer {
    class WordCountJob(input: Source, output: Source) extends PreparerJob {
      input
        .flatMapTo('word -> 'countedWord) { slice: KijiSlice[String] =>
            slice.cells.map { cell => cell.datum } }
        .groupBy('countedWord) { _.size }
        .map('countedWord -> 'entityId) {countedWord: String => EntityId(countedWord)}
        .map('size -> 'size) {size: Long => size.toString}
        .write(output)
    }

    override def prepare(input: Source, output: Source): Boolean = {
      new WordCountJob(input, output).run
      true
    }
  }

  class TrainWordCounter extends Trainer {
    class WordCountJob(input: Source, output: Source) extends TrainerJob {
      input
        .flatMapTo('word -> 'countedWord) { slice: KijiSlice[String] =>
        slice.cells.map { cell => cell.datum } }
        .groupBy('countedWord) { _.size }
        .map('countedWord -> 'entityId) {countedWord: String => EntityId(countedWord)}
        .map('size -> 'size) {size: Long => size.toString}
        .write(output)
    }

    override def train(input: Source, output: Source): Boolean = {
      new WordCountJob(input, output).run
      true
    }
  }

  class AverageTrainer extends Trainer {
    class AverageTrainerJob(input: Source, output: Source) extends TrainerJob {
      input
        .flatMapTo('word -> 'countedWord) { slice: KijiSlice[String] =>
        slice.cells.map { cell => cell.datum } }
        .mapTo('countedWord -> 'number) {countedWord: String => countedWord.toLong}
        .groupAll(_.average('number))
        .mapTo('number -> 'avgString) {avg: Double => avg.toString + " units"}
        .insert('entityId, EntityId("AVERAGE"))
        .write(output)
    }

    override def train(input: Source, output: Source): Boolean = {
      new AverageTrainerJob(input, output).run
      true
    }
  }
}
