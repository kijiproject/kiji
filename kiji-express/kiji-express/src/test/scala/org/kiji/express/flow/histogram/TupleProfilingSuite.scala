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

package org.kiji.express.flow.histogram

import java.io.File

import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.JobTest
import com.twitter.scalding.Mode
import com.twitter.scalding.Source
import com.twitter.scalding.Source
import com.twitter.scalding.Tsv
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.KijiJob
import org.kiji.express.flow.util.ResourceUtil

@RunWith(classOf[JUnitRunner])
class TupleProfilingSuite extends KijiSuite {
  test("Test even binning") {
    // Creates 4 bins:
    //  - test-even-histogram0: (-infinity, 0.0)
    //  - test-even-histogram1: [0.0, 10.0)
    //  - test-even-histogram2: [10.0, 20.0)
    //  - test-even-histogram3: [20.0, infinity)
    val equalWidthBinConfig: EqualWidthBinner = EqualWidthBinner(0.0, 10.0, 2)

    assert(0 === equalWidthBinConfig.binValue(-15.0))
    assert(0 === equalWidthBinConfig.binValue(-5.0))

    assert(1 === equalWidthBinConfig.binValue(0.0))
    assert(1 === equalWidthBinConfig.binValue(5.0))

    assert(2 === equalWidthBinConfig.binValue(10.0))
    assert(2 === equalWidthBinConfig.binValue(11.0))

    assert(3 === equalWidthBinConfig.binValue(20.0))
    assert(3 === equalWidthBinConfig.binValue(30.0))


    assert(Double.NegativeInfinity === equalWidthBinConfig.binBoundary(0))
    assert(0.0 === equalWidthBinConfig.binBoundary(1))
    assert(10.0 === equalWidthBinConfig.binBoundary(2))
    assert(20.0 === equalWidthBinConfig.binBoundary(3))
    assert(Double.PositiveInfinity === equalWidthBinConfig.binBoundary(4))
  }

  test("Test logarithmic binning") {
    // Creates bins:
    //  - test-log-histogram0: [0.0, 1.0)
    //  - test-log-histogram1: [1.0, 10.0)
    //  - test-log-histogram2: [10.0, 100.0)
    //  - test-log-histogram3: [100.0, 1000.0)
    //  - ...
    val logWidthBinConfig: LogWidthBinner = LogWidthBinner(10.0, 0.0, 1.0)

    val expectedException: Option[IllegalStateException] = TupleProfilingSuite.expectException({
      logWidthBinConfig.binValue(0.0)
    })
    assert(expectedException.nonEmpty, "0.0 should not be a valid value for logarithmic binning.")
    assert(expectedException.get.getMessage === "Expected value to be larger than 0: 0.0")

    assert(0 === logWidthBinConfig.binValue(0.5))
    assert(0 === logWidthBinConfig.binValue(0.9))

    assert(1 === logWidthBinConfig.binValue(1.0))
    assert(1 === logWidthBinConfig.binValue(5.0))

    assert(2 === logWidthBinConfig.binValue(10.0))
    assert(2 === logWidthBinConfig.binValue(50.0))

    assert(3 === logWidthBinConfig.binValue(100.0))
    assert(3 === logWidthBinConfig.binValue(500.0))

    // This can't be exactly 1000.0 due to a double-precision error.
    assert(4 === logWidthBinConfig.binValue(1000.1))
    assert(5 === logWidthBinConfig.binValue(15000.0))


    assert(0.0 === logWidthBinConfig.binBoundary(0))
    assert(1.0 === logWidthBinConfig.binBoundary(1))
    assert(10.0 === logWidthBinConfig.binBoundary(2))
    assert(100.0 === logWidthBinConfig.binBoundary(3))
    assert(1000.0 === logWidthBinConfig.binBoundary(4))
  }

  test("Test tuple size profiling") {
    val tempFile: File = File.createTempFile("size-histogram", ".csv")
    try {
      val data: Seq[Array[Int]] = Seq(
          // Should show up as 7 bytes (6 bytes of overhead, 1 byte for integer less than 127).
          Array(1),
          // Should show up as 8 bytes (6 bytes of overhead, 2 bytes for integers less than 127).
          Array(1, 2)
      )

      new JobTest(new TupleProfilingSuite.SizeProfilingJob(_))
          .arg("input", "input")
          .arg("output", "output")
          .arg("size-histogram", tempFile.getAbsolutePath)
          .source(Tsv("input"), data)
          .sink[Array[Int]](Tsv("output"))({ output: Seq[Array[Int]] => data == output})
          .run
          .runHadoop
          .counter("8", "SIZE_OF_TUPLES")({ counter: Long => assert(counter == 1L)})
          .counter("9", "SIZE_OF_TUPLES")({ counter: Long => assert(counter == 1L)})
          .finish

      // Validate the resulting csv file.
      ResourceUtil.doAndClose(io.Source.fromFile(tempFile))({ source =>
        val tempFileLines: Seq[String] = source.getLines().toList
        val expectedFile = Seq(
            "bin-id,lower-bound,upper-bound,value",
            "8,7.0,8.0,1",
            "9,8.0,9.0,1"
        )
        assert(expectedFile === tempFileLines)
      })
    } finally {
      tempFile.delete()
    }
  }

  test("Test tuple time profiling") {
    val tempFile: File = File.createTempFile("time-histogram", ".csv")
    try {
      // Number of milliseconds to sleep in the map phase.
      val data: Seq[Int] = Seq(10, 100, 1000)
      new JobTest(new TupleProfilingSuite.TimeProfilingJob(_))
          .arg("input", "input")
          .arg("output", "output")
          .arg("time-histogram", tempFile.getAbsolutePath)
          .source(Tsv("input"), data)
          .sink[Int](Tsv("output"))({ (output: Seq[Int]) => data == output})
          .run
          .runHadoop
          // These counters take nano-second time.
          .counter("8", "TIME_TO_PROCESS_TUPLES")({ counter: Long => assert(counter == 1L)})
          .counter("9", "TIME_TO_PROCESS_TUPLES")({ counter: Long => assert(counter == 1L)})
          .counter("10", "TIME_TO_PROCESS_TUPLES")({ counter: Long => assert(counter == 1L)})
          .finish

      // Validate the resulting csv file.
      ResourceUtil.doAndClose(io.Source.fromFile(tempFile))({ source =>
        val tempFileLines: Seq[String] = source.getLines().toList
        val expectedFile = Seq(
            "bin-id,lower-bound,upper-bound,value",
            "8,1.0E7,1.0E8,1",
            "9,1.0E8,1.0E9,1",
            "10,1.0E9,1.0E10,1"
        )
        assert(expectedFile === tempFileLines)
      })
    } finally {
      tempFile.delete()
    }
  }
}

object TupleProfilingSuite {
  /**
   * Runs a block of code expecting an exception. Use the type parameter to specify the expected
   * exception type.
   *
   * @tparam T is the expected exception type.
   * @param fn is the code block to execute.
   * @return an option containing an exception if one was produced.
   */
  def expectException[T <: Exception, O](fn: => O): Option[T] = {
    try {
      fn
      None
    } catch {
      case exception: T => Some(exception)
    }
  }

  /**
   * Example job for profiling tuples by size. Will profile the size of the first element of tuples.
   *
   * @param args to the job. These get parsed in from the command line by Scalding.
   */
  class SizeProfilingJob(args: Args) extends KijiJob(args) {
    def inputSource: Source = Tsv(args("input"))
    def outputSource: Source = Tsv(args("output"))

    private val sizeHistogram: HistogramConfig =
        HistogramConfig(
            name = "SIZE_OF_TUPLES",
            path = args("size-histogram"),
            binner = EqualWidthBinner(
                binStart = 0,
                binSize = 1,
                binCount = 10
            )
        )

    override def histograms: List[HistogramConfig] = super.histograms ++ Seq(sizeHistogram)

    inputSource
        .map(0 -> 'newData) { original: Array[Int] =>
          TupleProfiling.profileSize(sizeHistogram, original)
        }
        .write(outputSource)
  }

  /**
   * Example job for profiling function processing times. Will sleep for the int number of
   * milliseconds from the first element of the tuple.
   *
   * @param args to the job. These get parsed in from the command line by Scalding.
   */
  class TimeProfilingJob(args: Args) extends KijiJob(args) {
    def inputSource: Source = Tsv(args("input"))
    def outputSource: Source = Tsv(args("output"))

    private val timeHistogram: HistogramConfig =
        HistogramConfig(
            name = "TIME_TO_PROCESS_TUPLES",
            path = args("time-histogram"),
            binner = LogWidthBinner(logBase = 10.0)
        )

    override def histograms: List[HistogramConfig] = super.histograms ++ Seq(timeHistogram)

    inputSource
        .map(0 -> 'newData)(
            TupleProfiling.profileTime(
                timeHistogram,
                (input: Int) => {
                  Thread.sleep(input)
                  input
                }
            )
        )
        .write(outputSource)
  }
}
