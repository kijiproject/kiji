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

package org.kiji.express.examples

import scala.collection.mutable.Buffer

import cascading.tuple.Fields
import com.twitter.scalding.JobTest
import com.twitter.scalding.Tsv

import org.kiji.express.KijiSuite
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.util.Resources.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import java.io.File

class NewsgroupNaiveBayesClassifierSuite extends KijiSuite {
  val layout: KijiTableLayout = {
    KijiTableLayouts.getTableLayout("org/kiji/express/examples/layout/postings.json")
  }

  val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
    table.getURI().toString()
  }

  val outFile: String = "out-file"
  val weightsFile: String = "weights-file"

  // Make temp directory with folders corresponding to groups.
  // This is necessary because the NewsgroupClassifier takes as an argument a path to the root
  // directory of the newsgroup data, which it assumes contains one folder for each group.
  val dir: File = new File("/tmp/newsgroup-test-%s".format(System.currentTimeMillis()))
  require(dir.mkdir()) // Require directory creation succeeds
  val group1File: File = new File(dir.getAbsolutePath, "group1")
  require(group1File.mkdir()) // Require directory creation succeeds
  val group2File: File = new File(dir.getAbsolutePath, "group2")
  require(group2File.mkdir()) // Require directory creation succeeds

  // Fake output from TFIDF calculator that should run before the naive bayes classifier.
  val calculatedWeightsInput: List[(String, String, Int, Int, Double)] = List(
      ("group1", "hello", 2, 3, 2.0/3),
      ("group3", "hello", 1, 3, 1.0/3),
      ("group1", "world", 1, 2, 0.5),
      ("group2", "world", 1, 2, 0.5) )

  // Fake Kiji table input for the job. The rows are of form entityId, segment (0 or 1),
  // post text, and known tag (label)
  val testTableRowsInput: List[(EntityId, Seq[FlowCell[Int]], Seq[FlowCell[CharSequence]],
    Seq[FlowCell[CharSequence]])] = List(
      (EntityId("group1", "row05"),
          slice("info:segment", (0L, 0)),
          slice("info:post", (0L, "hello")),
          slice("info:group", (0L, "group1"))),
      (EntityId("group2", "row06"),
          slice("info:segment", (0L, 0)),
          slice("info:post", (0L, "world")),
          slice("info:group", (0L, "group1"))) )

  // Output is of form total correct, total counted, percent correct
  // Local mode ignores the header ..?
  def validateOutputLocal(outputBuffer: Buffer[(Int, Int, Double)]): Unit = {
    assert(outputBuffer.size == 1)
    assert(outputBuffer(0)._3 == 0.5)
  }

  // Output is of form total correct, total counted, percent correct
  // Hadoop mode keeps the header ..?
  def validateOutputHadoop(outputBuffer: Buffer[(Any, Any, Any)]): Unit = {
    assert(outputBuffer.size == 2)
    assert(outputBuffer(1)._3.asInstanceOf[String].toDouble == 0.5)
  }

  test("NewsgroupNaiveBayesClassifier runs in local mode") {
    JobTest(new NewsgroupClassifier(_))
      .arg("table", uri)
      .source(
          KijiInput.builder
              .withTableURI(uri)
              .withColumns(
                  "info:segment" -> 'segment,
                  "info:post" -> 'postText,
                  "info:group" -> 'tag)
              .build,
          testTableRowsInput)
      .arg("out-file", outFile)
      .arg("data-root", dir.getAbsolutePath)
      .arg("weights-file", weightsFile)
      .source(
          Tsv(weightsFile, new Fields("group", "word", "tf", "df", "weight"), skipHeader = true),
          // Converted via TupleConversions
          calculatedWeightsInput)
      .sink(Tsv(outFile, writeHeader = true))(validateOutputLocal)
      .run
      .finish
  }

  test("NewsgroupNaiveBayesClassifier runs in hadoop mode") {
    JobTest(new NewsgroupClassifier(_))
      .arg("table", uri)
      .source(
      KijiInput.builder
          .withTableURI(uri)
          .withColumns(
              "info:segment" -> 'segment,
              "info:post" -> 'postText,
              "info:group" -> 'tag)
          .build,
          testTableRowsInput)
      .arg("out-file", outFile)
      .arg("data-root", dir.getAbsolutePath)
      .arg("weights-file", weightsFile)
      .source(
          Tsv(weightsFile, new Fields("group", "word", "tf", "df", "weight"), skipHeader = true),
      // Converted via TupleConversions
      calculatedWeightsInput)
      .sink(Tsv(outFile, writeHeader = true))(validateOutputHadoop)
      .runHadoop
      .finish
  }
}
