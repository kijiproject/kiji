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

import com.twitter.scalding.JobTest
import com.twitter.scalding.Tsv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

@RunWith(classOf[JUnitRunner])
class NewsgroupTFIDFSuite extends KijiSuite {
  val layout: KijiTableLayout = {
    KijiTableLayouts.getTableLayout("org/kiji/express/examples/layout/postings.json")
  }

  val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
    table.getURI().toString()
  }

  val testInput: List[(EntityId, Seq[FlowCell[Int]], Seq[FlowCell[CharSequence]])] = List(
    ( EntityId("group1", "row01"),
      slice("info:segment", (0L, 1)),
      slice("info:post", (0L, "hello hello hello     hello")) ),
    ( EntityId("group1", "post02"),
      slice("info:segment", (0L, 1)),
      slice("info:post", (0L, "hello    \nworld")) ),
    ( EntityId("group2", "post03"),
      slice("info:segment", (0L, 1)),
      slice("info:post", (0L, "world")) ),
    ( EntityId("group3", "post04"),
      slice("info:segment", (0L, 1)),
      slice("info:post", (0L, "hello")) ))

  val outFile: String = "out-file"

  // Test output is of form: groupName, word, TF, DF, TFIDF
  def validateTestLocal(outputBuffer: Buffer[(String, String, Int, Int, Double)]): Unit = {
    assert(4 === outputBuffer.size)
    assert(outputBuffer.contains(("group1", "hello", 2, 3, 2.0/3)))
    assert(outputBuffer.contains(("group3", "hello", 1, 3, 1.0/3)))
    assert(outputBuffer.contains(("group1", "world", 1, 2, 0.5)))
    assert(outputBuffer.contains(("group2", "world", 1, 2, 0.5)))
  }

  // Test output is of form: groupName, word, TF, DF, TFIDF
  def validateTestHadoop(outputBuffer: Buffer[(Any, Any, Any, Any, Any)]): Unit = {
    assert(5 === outputBuffer.size)
    assert(outputBuffer.contains(("group1", "hello", 2.toString, 3.toString, (2.0/3).toString)))
    assert(outputBuffer.contains(("group3", "hello", 1.toString, 3.toString, (1.0/3).toString)))
    assert(outputBuffer.contains(("group1", "world", 1.toString, 2.toString, 0.5.toString)))
    assert(outputBuffer.contains(("group2", "world", 1.toString, 2.toString, 0.5.toString)))
  }

  test("NewsgroupTFIDF runs in local mode.") {
    JobTest(new NewsgroupTFIDF(_))
      .arg("out-file", outFile)
      .arg("table", uri)
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("info:segment" -> 'segment, "info:post" -> 'postText)
          .build,
          testInput)
      .sink(Tsv(outFile, writeHeader = true))(validateTestLocal)
      .run
      .finish
  }

  test("NewsgroupTFIDF runs in hadoop mode.") {
    JobTest(new NewsgroupTFIDF(_))
      .arg("table", uri)
      .arg("out-file", outFile)
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("info:segment" -> 'segment, "info:post" -> 'postText)
          .build,
          testInput)
      .sink(Tsv(outFile, writeHeader = true))(validateTestHadoop)
      .runHadoop
      .finish
  }
}
