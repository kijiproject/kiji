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

import com.twitter.scalding.Hdfs
import com.twitter.scalding.Source
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.All
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.modeling.Preparer
import org.kiji.modeling.Trainer
import org.kiji.modeling.config.KeyValueStoreSpec
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiOutputSpec
import org.kiji.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.PrepareEnvironment
import org.kiji.modeling.config.ScoreEnvironment
import org.kiji.modeling.config.TrainEnvironment
import org.kiji.modeling.impl.KeyValueStoreImplSuite
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class ModelExecutorSuite extends KijiSuite {
  test("A ModelExecutor is properly created") {
    val modelDef: ModelDefinition = ModelDefinition(
        name = "test-model-definition",
        version = "1.0",
        preparerClass = Some(classOf[ModelExecutorSuite.PrepareWordCounter]),
        trainerClass = Some(classOf[ModelExecutorSuite.TrainWordCounter]),
        scoreExtractorClass = Some(classOf[ScoreProducerSuite.DoublingExtractor]),
        scorerClass = Some(classOf[ScoreProducerSuite.UpperCaseScorer]))
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
            timeRange=All,
            columnsToFields = Map(QualifiedColumnInputSpec.builder
                .withColumn("family", "column1")
                .build -> 'field)),
          KijiSingleColumnOutputSpec(uri.toString, QualifiedColumnOutputSpec.builder
              .withColumn("family", "column2")
              .build),
          keyValueStoreSpecs = Seq(
            KeyValueStoreSpec(
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
        preparerClass = Some(classOf[ModelExecutorSuite.PrepareWordCounter]))

    val inputUri: KijiURI = doAndRelease(kiji.openTable("input_table")) { table: KijiTable =>
      table.getURI
    }

    doAndRelease(kiji.openTable("output_table")) { table: KijiTable =>
      val outputUri: KijiURI = table.getURI

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "prepare-model-environment",
        version = "1.0",
        prepareEnvironment = Some(PrepareEnvironment(
            inputSpec = Map("input" ->
                KijiInputSpec(
                  inputUri.toString,
                  timeRange=All,
                  columnsToFields =
                      Map(QualifiedColumnInputSpec.builder
                          .withColumn("family", "column1")
                          .build -> 'word))),
            outputSpec = Map("output" ->
                KijiOutputSpec(
                    tableUri = outputUri.toString,
                    fieldsToColumns = Map('size -> QualifiedColumnOutputSpec.builder
                        .withColumn("family", "column")
                        .build)
            )),
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
      trainerClass = Some(classOf[ModelExecutorSuite.TrainWordCounter]))

    val inputUri: KijiURI = doAndRelease(kiji.openTable("input_table")) { table: KijiTable =>
      table.getURI
    }

    doAndRelease(kiji.openTable("output_table")) { table: KijiTable =>
      val outputUri: KijiURI = table.getURI

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "prepare-model-environment",
        version = "1.0",
        trainEnvironment = Some(TrainEnvironment(
          inputSpec = Map("input" ->
              KijiInputSpec(
                  inputUri.toString,
                  timeRange=All,
                  columnsToFields =
                      Map(QualifiedColumnInputSpec.builder
                          .withColumn("family", "column1")
                          .build -> 'word))),
          outputSpec = Map("output" ->
              KijiOutputSpec(
                  tableUri = outputUri.toString,
                  fieldsToColumns = Map('size -> QualifiedColumnOutputSpec.builder
                      .withColumn("family", "column")
                      .build)
          )),
          keyValueStoreSpecs = Seq()
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
      com.twitter.scalding.Mode.mode = Hdfs(
          strict = false,
          conf = HBaseConfiguration.create())

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
    // The prepare phase will count the number of occurrences of values in the 'family:column1'
    // column of the 'input_table' and write this to the 'family:column' column of the
    // 'prepare_output_table'.
    //
    // The train phase will read the 'family:column' column of the 'prepare_output_table' and
    // calculate an average of the generated counts, and write this value to the 'family:column1'
    // column of the 'train_output_table' at a row with entityId "AVERAGE".
    //
    // The score phase uses an extractor that doubles the string it is passed and an scorer that
    // takes a string and produces an upper case version of that string. It takes as input
    // the average calculated by the trainerClass. It writes the output to the output_table at a row
    // with entityId "FINAL".

    // Setup the test environment.
    val kiji: Kiji = {
      val testLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS).getDesc
      testLayoutDesc.setName("input_table")

      val prepareOutputLayoutDesc: TableLayoutDesc = layout(KijiTableLayouts.SIMPLE).getDesc
      prepareOutputLayoutDesc.setName("prepare_output_table")

      val trainOutputLayoutDesc: TableLayoutDesc =
          layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS).getDesc
      trainOutputLayoutDesc.setName("train_output_table")

      new InstanceBuilder("default")
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
    }

    val modelDefinition: ModelDefinition = ModelDefinition(
        name = "prepare-model-definition",
        version = "1.0",
        preparerClass = Some(classOf[ModelExecutorSuite.PrepareWordCounter]),
        trainerClass = Some(classOf[ModelExecutorSuite.AverageTrainer]),
        scoreExtractorClass = Some(classOf[ScoreProducerSuite.DoublingExtractor]),
        scorerClass = Some(classOf[ScoreProducerSuite.UpperCaseScorer]))

    val sideDataPath: Path = KeyValueStoreImplSuite.generateAvroKVRecordKeyValueStore()

    val inputUri: KijiURI = doAndRelease(kiji.openTable("input_table")) { table: KijiTable =>
      table.getURI
    }

    val prepareOutputUri: KijiURI = doAndRelease(kiji.openTable("prepare_output_table")) {
      table: KijiTable => table.getURI
    }

    doAndRelease(kiji.openTable("train_output_table")) { table: KijiTable =>
      val trainOutputUri: KijiURI = table.getURI

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "prepare-model-environment",
        version = "1.0",
        prepareEnvironment = Some(PrepareEnvironment(
            inputSpec = Map("input" ->
                KijiInputSpec(
                    tableUri = inputUri.toString,
                    timeRange=All,
                    columnsToFields =
                        Map(QualifiedColumnInputSpec.builder
                            .withColumn("family", "column1")
                            .build -> 'word))),
            outputSpec = Map("output" ->
                KijiOutputSpec(
                    tableUri = prepareOutputUri.toString,
                    fieldsToColumns = Map('size -> QualifiedColumnOutputSpec.builder
                        .withColumn("family", "column")
                        .build))),
            keyValueStoreSpecs = Seq())),
        trainEnvironment = Some(TrainEnvironment(
            inputSpec = Map("input" ->
                KijiInputSpec(
                    tableUri = prepareOutputUri.toString,
                    timeRange=All,
                    columnsToFields =
                        Map(QualifiedColumnInputSpec.builder
                            .withColumn("family", "column")
                            .build -> 'word))),
            outputSpec = Map("output" ->
                KijiOutputSpec(
                    tableUri = trainOutputUri.toString,
                    fieldsToColumns = Map('avgString ->
                        QualifiedColumnOutputSpec.builder
                            .withColumn("family", "column1")
                            .build))),
            keyValueStoreSpecs = Seq())),
        scoreEnvironment = Some(ScoreEnvironment(
            inputSpec = KijiInputSpec(
                tableUri = trainOutputUri.toString,
                timeRange=All,
                columnsToFields =
                    Map(QualifiedColumnInputSpec.builder
                        .withColumn("family", "column1")
                        .build -> 'field)),
            outputSpec = KijiSingleColumnOutputSpec(
                tableUri = trainOutputUri.toString,
                QualifiedColumnOutputSpec.builder
                    .withColumn("family", "column2")
                    .build),
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
    class WordCountJob(input: Map[String, Source], output: Map[String, Source]) extends
        PreparerJob {
      input("input")
        .flatMapTo('word -> 'countedWord) { slice: Seq[FlowCell[CharSequence]] =>
            slice.map { cell => cell.datum.toString } }
        .groupBy('countedWord) { _.size }
        .map('countedWord -> 'entityId) { countedWord: String => EntityId(countedWord) }
        .map('size -> 'size) { size: Long => size.toString }
        .write(output("output"))
    }

    override def prepare(input: Map[String, Source], output: Map[String, Source]): Boolean = {
      new WordCountJob(input, output).run
      true
    }
  }

  class TrainWordCounter extends Trainer {
    class WordCountJob(input: Map[String, Source], output: Map[String,
        Source]) extends TrainerJob {
      input("input")
        .flatMapTo('word -> 'countedWord) { slice: Seq[FlowCell[CharSequence]] =>
          slice.map { cell => cell.datum.toString }
        }
        .groupBy('countedWord) { _.size }
        .map('countedWord -> 'entityId) { countedWord: String => EntityId(countedWord) }
        .map('size -> 'size) { size: Long => size.toString }
        .write(output("output"))
    }

    override def train(input: Map[String, Source], output: Map[String, Source]): Boolean = {
      new WordCountJob(input, output).run
      true
    }
  }

  class AverageTrainer extends Trainer {
    class AverageTrainerJob(input: Map[String, Source], output: Map[String, Source]) extends
        TrainerJob {
      input("input")
        .flatMapTo('word -> 'countedWord) { slice: Seq[FlowCell[CharSequence]] =>
          slice.map { cell => cell.datum.toString }
        }
        .mapTo('countedWord -> 'number) { countedWord: String => countedWord.toLong }
        .groupAll(_.average('number))
        .mapTo('number -> 'avgString) { avg: Double => avg.toString + " units" }
        .insert('entityId, EntityId("AVERAGE"))
        .write(output("output"))
    }

    override def train(input: Map[String, Source], output: Map[String, Source]): Boolean = {
      new AverageTrainerJob(input, output).run
      true
    }
  }
}
