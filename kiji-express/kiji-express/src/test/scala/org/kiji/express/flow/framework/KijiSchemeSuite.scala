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

package org.kiji.express.flow.framework

import scala.collection.mutable.Buffer

import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.KijiOutput
import org.kiji.express.flow.KijiSource
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

@RunWith(classOf[JUnitRunner])
class KijiSchemeSuite extends KijiSuite {
  test("A KijiJob can write to a fully qualified column in a column family.") {
    val layout = KijiTableLayout.newLayout(
      KijiTableLayouts.getLayout("layout/avro-types-1.3.json"))
    val testTable = makeTestKijiTable(layout)
    val tableUri = testTable.getURI.toString
    val input = List((0, "1"))

    def validateOutput(b: Buffer[Any]): Unit = {
      println(b)
      b === Seq(1)
    }

    JobTest(new KijiSchemeSuite.IdentityJob(_))
      .arg("input", "temp")
      .arg("output", tableUri)
      .source(TextLine("temp"), input)
      .sink(KijiSchemeSuite.output(tableUri)) (validateOutput)
      .runHadoop
  }
}

object KijiSchemeSuite {
  // Construct the KijiOutput used in IdentityJob, given a table URI.
  def output(uri: String): KijiSource = KijiOutput.builder
    .withTableURI(uri)
    .withColumnSpecs(Map('line -> QualifiedColumnOutputSpec.builder
    .withFamily("searches").withQualifier("dummy-qualifier").build)).build

  class IdentityJob(args: Args) extends Job(args) {
    TextLine(args("input"))
        .map('offset -> 'entityId) {offset: Int => EntityId(offset.toString)}
        .map('line -> 'line) { line: String => line.toInt }
        .write(output(args("output")))
  }
}
