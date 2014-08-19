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
import java.io.PrintWriter
import java.util.Properties

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.SortedMap

import cascading.flow.FlowProcess
import cascading.kryo.KryoSerialization
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.twitter.scalding.RuntimeStats
import com.twitter.scalding.UniqueID
import org.apache.hadoop.conf.Configuration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * Provides support for generating profiling histograms. Currently 2 types of histograms are
 * supported:
 * <ul>
 *   <li>sizes of tuples passing through a point in a scalding job</li>
 *   <li>time to complete an operation in a scalding job</li>
 * </ul>
 *
 * Size based profiling:
 * <ul>
 *   <li>Produces tuple size histogram for tuples at a specific point in the job.</li>
 *   <li>
 *     Requires that input/output data is serializable by kryo (already an assumption that scalding
 *     makes).
 *   </li>
 *   <li>Histogram bin size unit will be in bytes.</li>
 * </ul>
 *
 * Time based profiling:
 * <ul>
 *   <li>Produces a histogram of the processing time of the operation per input.</li>
 *   <li>Histogram bin size unit will be in milliseconds.</li>
 * </ul>
 *
 * The produced histograms will use the following naming scheme for their counters:
 *   histogram-name1
 *
 * Histograms will be written to disk at the specified location. /dev/stdout, /dev/stderr can be
 * used to redirect output to the terminal.
 *
 *
 * Example:
 * <pre><code>
 * class TupleProfilingExampleJob(args: Args) extends KijiJob(args) {
 *   // Define some dummy sources.
 *   def inputSource: Source = ???
 *   def outputSource: Source = ???
 *
 *   // Create some dummy processing methods.
 *   def processData[I, O](input: I): O = ???
 *
 *   // Define the histograms to use.
 *   val sizeHistogram: HistogramConfig =
 *       HistogramConfig(
 *           name="size-of-tuples-map-1",
 *           path=args("before-size-histogram"),
 *           binner=EqualWidthBinner(binSize=10, binCount=500)
 *       )
 *   val timeHistogram: HistogramConfig =
 *       HistogramConfig(
 *           name="time-to-process-map-1",
 *           path=args("time-histogram"),
 *           binner=LogWidthBinner(logBase=math.E)
 *       )
 *
 *   inputSource
 *       // Measure the size of tuples in a map.
 *       .map('some_input_field, 'some_output_field)({ input =>
 *         TupleProfiling.profileSize(sizeHistogram, input)
 *
 *         // Can do additional processing here and can call profileSize again in here to generate
 *         // another histogram.
 *         processData(input)
 *       })
 *       // Measure the time to filter tuples.
 *       .filter('some_output_field)(
 *           TupleProfiling.profileTime(timeHistogram, processData)
 *       )
 *       .write(outputSource)
 * }
 * </code></pre>
 */
@ApiAudience.Framework
@ApiStability.Experimental
object TupleProfiling {
  /**
   * Profiles the sizes of the provided tuples. Requires that the provided data is serializable by
   * Kryo.
   *
   * @tparam T is the type of the tuple.
   * @param histogramConfig defining the histogram (bin settings and name).
   * @param tuple to profile.
   * @param uniqueIdCont used to identify the job that this profile method is being used within.
   *     This is used to get a kryo configured as it would be for cascading.
   * @return the provided tuple.
   */
  def profileSize[T]
      (histogramConfig: HistogramConfig, tuple: T)
      (implicit uniqueIdCont: UniqueID): T = {
    // Serialize the tuple using kryo.
    val flowProcess: FlowProcess[_] = RuntimeStats.getFlowProcessForUniqueId(uniqueIdCont.get)
    val kryo: Kryo = flowProcess.getConfigCopy match {
      case hadoopConfiguration: Configuration => {
        new KryoSerialization(hadoopConfiguration).populatedKryo()
      }
      case localConfiguration: Properties => {
        val kryoConfiguration: Configuration = localConfiguration
            .asScala
            .foldLeft(new Configuration()) { (conf: Configuration, entry: (AnyRef, AnyRef)) =>
              val (key, value) = entry
              conf.set(key.toString, value.toString)
              conf
            }
        new KryoSerialization(kryoConfiguration).populatedKryo()
      }
    }
    val output: Output = new Output(4096, Int.MaxValue)
    val tupleSize: Double = try {
      kryo.writeClassAndObject(output, tuple)
      output.total()
    } finally {
      output.close()
    }

    // Bucket the size of the byte array into the provided histogram bins.
    histogramConfig.incrementBinCount(tupleSize, uniqueIdCont)

    tuple
  }

  /**
   * Profiles the processing time for the provided function.
   *
   * @tparam I is the input type of the function.
   * @tparam O is the output type of the function.
   * @param histogramConfig defining the histogram (bin settings and name).
   * @param fn to profile.
   * @return a function that will run and time the provided function.
   */
  def profileTime[I, O]
      (histogramConfig: HistogramConfig, fn: I => O)
      (implicit uniqueIdCont: UniqueID): I => O = {
    // Return a function that adds timing to the provided function.
    { input: I =>
      // Run the function recording the start and end timestamps.
      val startTime: Long = System.nanoTime()
      val returnValue: O = fn(input)
      val stopTime: Long = System.nanoTime()
      val elapsedTime = stopTime - startTime

      // Bucket the time to process into the provided histogram bins.
      histogramConfig.incrementBinCount(elapsedTime, uniqueIdCont)

      returnValue
    }
  }

  /**
   * Writes a histogram to a file.
   *
   * @param histogram to write. Also specifies the location of the file to write to.
   * @param counters resulting from running an express flow.
   */
  def writeHistogram(histogram: HistogramConfig, counters: Set[(String, String, Long)]): Unit = {
    // Find all counters for this histogram.
    val binMap = counters
        .collect({
          case (counterName, counterBin, counterValue) if counterName == histogram.name => {
            (counterBin.toInt, counterValue)
          }
        })
        .toSeq

    val sortedBinMap: SortedMap[Int, Long] = SortedMap(binMap: _*)

    val histogramFile = new File(histogram.path)
    if (histogramFile.exists()) {
      histogramFile.delete()
    }
    val writer: PrintWriter = new PrintWriter(histogramFile)
    try {
      // Print the header.
      writer.println("bin-id,lower-bound,upper-bound,value")

      // Print the counter values.
      sortedBinMap
          .foreach({ counter: (Int, Long) =>
            val (counterId, counterValue) = counter

            val lowerBound: Double = histogram.binLowerBound(counterId)
            val upperBound: Double = histogram.binUpperBound(counterId)
            writer.println(s"$counterId,$lowerBound,$upperBound,$counterValue")
          })
    } finally {
      writer.flush()
      writer.close()
    }
  }
}
