/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.express.flow.framework.serialization

import scala.collection.mutable

import org.kiji.express.KijiJobSuiteSampleData
import org.kiji.express.KijiSuite
import org.kiji.express.flow._
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.schema.KijiTable

import cascading.pipe.Pipe
import com.twitter.scalding.JobTest
import com.twitter.scalding.Tsv
import com.twitter.scalding.Args
import org.apache.avro.generic.GenericRecord
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransientStreamSerializationSuite extends KijiSuite {
  import KijiJobSuiteSampleData._

  val uri: String = ResourceUtil.doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
    table.getURI.toString
  }

  val slices: List[Seq[FlowCell[GenericRecord]]] = genericInputs.map { record: GenericRecord =>
    List(FlowCell("family", "simple", version = 1111, datum = record))
  }

  val input: List[(EntityId, Seq[FlowCell[GenericRecord]])] = eids.zip(slices)

  def validateTransientSeqJoin(
      output: mutable.Buffer[(String, String)]): Unit = {
    Assert.assertEquals(slices.size, output.size)
    for (i: Int <- Range(0, slices.size)) {
      Assert.assertEquals(slices(i)(0).toString(), output(i)._2)
    }
  }

  private def jobTest =
      JobTest(new TransientStreamSerializationSuite.TransientStreamSerializationJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(
          KijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
            .withColumn("family", "simple")
            .withPagingSpec(PagingSpec.Cells(1))
            .build
                -> 'transientSeq1)
            .build,
          input)
        .source(
          KijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
            .withColumn("family", "simple")
            .withPagingSpec(PagingSpec.Cells(1))
            .build
                -> 'transientSeq2)
            .build,
          input)
        .sink(Tsv("outputFile"))(validateTransientSeqJoin)

  test("A TransientStream can be serialized when necessary in a reduce phase, in local mode.") {
    // Local mode has always worked; this test is just here for completeness.
    jobTest.run.finish
  }

  test("A TransientStream can be serialized when necessary in a reduce phase, in hadoop mode.") {
    // Hadoop mode was the one where it used to fail.
    jobTest.runHadoop.finish
  }
}

object TransientStreamSerializationSuite {
  /**
   * Joins two pipes containing fields of type TransientStream.  This used to fail when we
   * upgraded to Scalding 0.9.x and Scala 2.10, before we added a custom serializer for
   * TransientStream.
   *
   * @param args to the job.  This job expects "input", which is a Kiji table URI,
   *             that both pipes will read from, and "output", which is a location for a Tsv,
   *             that the result of the join will be written to.
   */
  class TransientStreamSerializationJob(args: Args) extends KijiJob(args) {
    val pipe1: Pipe = KijiInput.builder
      .withTableURI(args("input"))
      .withColumnSpecs(
          QualifiedColumnInputSpec.builder
            .withColumn("family", "simple")
            .withPagingSpec(PagingSpec.Cells(1))
            .build
          -> 'transientSeq1)
      .build

    val pipe2: Pipe = KijiInput.builder
      .withTableURI(args("input"))
      .withColumnSpecs(
        QualifiedColumnInputSpec.builder
          .withColumn("family", "simple")
          .withPagingSpec(PagingSpec.Cells(1))
          .build
        -> 'transientSeq2)
      .build

    pipe1.joinWithSmaller('entityId -> 'entityId, pipe2)
      .discard('transientSeq2)
      .map('transientSeq1 -> 'elem1) { transientSeq: Seq[FlowCell[_]] =>
        transientSeq(0)
      }
      .discard('transientSeq1)
      .write(Tsv(args("output")))
  }
}
