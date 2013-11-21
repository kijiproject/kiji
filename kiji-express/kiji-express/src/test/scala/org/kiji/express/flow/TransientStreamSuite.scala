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

package org.kiji.express.flow

import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.StreamView
import scala.collection.mutable

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite

@RunWith(classOf[JUnitRunner])
class TransientStreamSuite extends KijiSuite {

  // C(i, n) is a complexity measure of the number of iterators requested from the TransientStream,
  // i, and the number of elements requested from all iterators, n. N is the number of elements
  // held by the underlying iterators of the TransientStream.

  val N = 100

  test("Realizing a TransientStream is C(1, N) and C(0, 0) thereafter") {
    assertComplexity(1, N, { tstream =>
      tstream.force
      tstream.force
    }, runs = 5, threshold = 4)
  }

  test("Realizing a TransientStream is C(1, N) every time if the cached stream is collected") {
    assertComplexity(2, 2 * N, { tstream =>
      tstream.force
      System.gc()
      tstream.force
    }, runs = 5, threshold = 4)
  }

  test("TransientStream will not hold onto its cached stream if memory pressure occurs") {
    // The only (sure) way to test this is to make sure an OOME doesn't occur
    val N =  20 * 1000 * 1000 // Will blow up with a 1GB heap and force
    assertComplexity(1, N, { tstream =>
      tstream.foreach { x => }
    }, size = N)
  }

  test("view will return a StreamView in C(0, 0)") {
    assertComplexity(0, 0, tstream => tstream.view)
    val tstream = new TransientStream(() => (1 to N).iterator)
    assert(tstream.view.isInstanceOf[StreamView[_, _]], "Class of view is incorrect")
  }

  test("force is C(1, N)") { assertComplexity(1, N, tstream => tstream.force) }

  test("head is C(1, 1)") { assertComplexity(1, 1, tstream => tstream.head) }

  test("tail is C(1, 2)") { assertComplexity(1, 2, tstream => tstream.tail) }

  test("tail * m is C(1, m + 1)") {
    assertComplexity(1, 5, tstream => tstream.tail.tail.tail.tail)
  }

  test("tail * m then force is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.tail.tail.tail.tail.force)
  }

  test("force then tail * m is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.force.tail.tail.tail.tail)
  }

  test("take(m) is C(1, 1)") { assertComplexity(1, 1, tstream => tstream.take(42)) }

  test("take(m) then force is C(1, m)") {
    assertComplexity(1, 42, tstream => tstream.take(42).force)
  }

  test("drop(m) is C(1, m + 1)") { assertComplexity(1, 43, tstream => tstream.drop(42)) }

  test("drop(m) then force is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.drop(42).force)
  }

  test("takeRight and dropRight are C(1, N)") {
    assertComplexity(1, N, tstream => tstream.takeRight(2))
    assertComplexity(1, N, tstream => tstream.dropRight(2))
  }

  test("dropWhile is C(1, O(N))") {
    assertComplexity(1, 42, tstream => tstream.dropWhile(x => x < 42))
  }

  test("dropWhile then force is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.dropWhile(x => x < 42).force)
  }

  test("appends and concats to tail are C(1, 1)") {
    assertComplexity(1, 1, tstream => tstream ++ List(1, 2, 3))
    assertComplexity(1, 1, tstream => tstream.:+(1))
  }

  test("append to head is C(0, 0)") {
    assertComplexity(0, 0, tstream => tstream.+:(1))
  }

  test("concat to head is C(1, N)") {
    assertComplexity(1, N, tstream => tstream ++: List(1, 2, 3))
  }

  test("fold is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.foldLeft(0)(sum))
    assertComplexity(1, N, tstream => tstream.foldRight(0)(sum))
    assertComplexity(1, N, tstream => tstream./:(0)(sum))
    assertComplexity(1, N, tstream => tstream.:\(0)(sum))
    assertComplexity(1, N, tstream => tstream.aggregate(0)(sum, sum))
  }

  test("reduce is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.reduce(sum))
    assertComplexity(1, N, tstream => tstream.reduceLeft(sum))
    assertComplexity(1, N, tstream => tstream.reduceRight(sum))
    assertComplexity(1, N, tstream => tstream.reduceLeftOption(sum))
    assertComplexity(1, N, tstream => tstream.reduceRightOption(sum))
  }

  test("flatten is C(1, 1)") {
    assertComplexity(1, 1, tstream => tstream.map(x => List(x)).flatten)
  }

  test("map is C(0, 0)") {
    assertComplexity(1, 1, tstream => tstream.map(x => x + 1))
    assertComplexity(1, 1, tstream => tstream.flatMap(x => List(x, x)))
  }

  test("reverseMap is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.reverseMap(x => x + 1))
  }

  test("map then force is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.map(x => x + 1).force)
    assertComplexity(1, N, tstream => tstream.flatMap(x => List(x, x)).force)
    assertComplexity(1, N, tstream => tstream.reverseMap(x => x + 1).force)
  }

  test("map then toList is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.map(x => x + 1).toList)
    assertComplexity(1, N, tstream => tstream.flatMap(x => List(x, x)).toList)
    assertComplexity(1, N, tstream => tstream.reverseMap(x => x + 1).toList)
  }

  test("filter is C(1, x) where x is the index of the first non-filtered element") {
    assertComplexity(1, 2, tstream => tstream.filter(x => x % 2 == 0))
    assertComplexity(1, 2, tstream => tstream.filterNot(x => x % 2 != 0))
    assertComplexity(1, 2, tstream => tstream.collect { case x if x % 2 == 0 => x })
  }

  test("filter is C(1, N) where x is the index of the first non-filtered element") {
    assertComplexity(1, N, tstream => tstream.filter(x => x % 2 == 0).force)
    assertComplexity(1, N, tstream => tstream.filterNot(x => x % 2 == 0).force)
  }

  test("filter then toList is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.filter(x => x % 2 == 0).toList)
    assertComplexity(1, N, tstream => tstream.filterNot(x => x % 2 == 0).toList)
  }

  test("membership ops are C(1, O(N))") {
    assertComplexity(1, 42, tstream => tstream.collectFirst { case x if x == 42 => x })
    assertComplexity(1, 42, tstream => tstream.contains(42))
    assertComplexity(1, 42, tstream => tstream.indexOf(42))
    assertComplexity(1, 42, tstream => tstream.indexWhere(_ == 42))
    assertComplexity(1, 42, tstream => tstream.indexWhere(_ == 42, 3))
    assertComplexity(1, 42, tstream => tstream.exists(_ == 42))
    assertComplexity(1, 42, tstream => tstream.find(_ == 42))
    assertComplexity(1, 42, tstream => tstream.containsSlice(List(41, 42)))
    assertComplexity(1, 42, tstream => tstream.indexOfSlice(List(41, 42)))
  }

  test("last membership ops are C(1, N)") {
    assertComplexity(1, N, tstream => tstream.lastIndexOfSlice(List(41, 42)))
    assertComplexity(1, N, tstream => tstream.lastIndexOfSlice(List(42), 10))
  }

  test("size is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.size)
    assertComplexity(1, N, tstream => tstream.length)
  }

  test("copy ops are C(1, L) where L is the number of elements to copy") {
    assertComplexity(1, 10, tstream => tstream.copyToArray(Array.ofDim[Int](10)))
    assertComplexity(1, 10, tstream => tstream.copyToArray(Array.ofDim[Int](10), 0))
    assertComplexity(1, 10, tstream => tstream.copyToArray(Array.ofDim[Int](10), 0, 10))
    assertComplexity(1, 10, tstream => tstream.copyToBuffer(mutable.Buffer[Int]()), size = 10)
  }

  test("updated(m, _) is C(1, m + 1)") {
    assertComplexity(1, 43, tstream => tstream.updated(42, 99))
  }

  test("corresponds is C(1, O(Min(N + 1, L))) where L is the length of the other Seq") {
    assertComplexity(1, 6, tstream => tstream.corresponds(1 to 5)(_ equals _))
  }

  test("combinations is C(1, N) if no garbage is collected") {
    assertComplexity(1, N, tstream => tstream.combinations(3))
  }

  test("permutations is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.permutations)
  }

  test("groupBy is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.groupBy(x => x % 4 == 0))
  }

  test("grouped is C(0, 0)") {
    assertComplexity(0, 0, tstream => tstream.grouped(2))
  }

  test("zip is C(1, 1)") {
    assertComplexity(1, 1, tstream => tstream.zip(1 to N))
    assertComplexity(1, 1, tstream => tstream.zipWithIndex)
  }

  test("zipAll is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.zipAll(1 to N - 10, 99, 99))
  }

  test("unzip is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.zipWithIndex.unzip)
  }

  test("sorted is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.sorted)
  }

  test("distinct is C(1, 1)") {
    assertComplexity(1, 1, tstream => tstream.distinct)
  }

  test("distinct then force is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.distinct.force)
  }

  test("set union is C(1, 1)") {
    assertComplexity(1, 1, tstream => tstream.union(List(1, 2, 3, 99)))
  }

  test("set intersect and diff are C(1, N)") {
    assertComplexity(1, N, tstream => tstream.intersect(List(1, 2, 3, 99)))
    assertComplexity(1, N, tstream => tstream.diff(List(1, 2, 3, 99)))
  }

  test("patch is C(0, O(N))") {
    assertComplexity(1, 43, tstream => tstream.patch(42, List(1, 2, 3), 10))
  }

  test("startsWith is C(1, _)") {
    assertComplexity(1, 1, tstream => tstream.startsWith(List(10)))
    assertComplexity(1, 4, tstream => tstream.startsWith(List(10), 3))
  }

  test("endsWith is C(1, N)") {
    assertComplexity(1, N, tstream => tstream.endsWith(List(10)))
  }

  def sum(a: Int, b: Int): Int = a + b

  def countingTSeq(size: Int): (AtomicLong, AtomicLong, TransientStream[Int]) = {
    val i = new AtomicLong() // number of iterators requested.
    val n = new AtomicLong() // number of elements requested across all iterators.
    def genItr(): Iterator[Int] = {
      i.incrementAndGet()
      val s = size // because Scala bug
      new Iterator[Int] {
        val itr = (1 to s).iterator
        def hasNext: Boolean = itr.hasNext
        def next(): Int = { n.incrementAndGet() ; itr.next() }
      }
    }
    (i, n, new TransientStream(genItr))
  }

  /**
   * Test the whether the expected lower-bound complexity of TransientStream is met. Complexity is
   * measured in terms of i and n, number of iterators over the backing collection created and total
   * number of `next` calls among all created iterators, respectively.  The complexity of a given
   * operation on a TransientStream is dependent on whether it has a cached stream already, and
   * possibly whether garbage collection happens during the run.
   *
   * Accordingly, this method optionally takes runs and threshold params which should be specified
   * if the test is non-deterministic.  At least threshold number of runs out of the total runs
   * must pass for the assert to pass.
   *
   * @param i expected number of iterators created
   * @param n expected number of next calls across all iterators
   * @param op operation to run with a new TransientStream[Int]
   * @param debug whether to print complexity results of each run
   * @param runs number of runs
   * @param threshold number of runs which must pass to be successfull
   * @param size of TransientSequence to test with
   */
  def assertComplexity(
      i: Long,
      n: Long,
      op: TransientStream[Int] => Unit,
      debug: Boolean = false,
      runs: Int = 1, // Assume deterministic by default
      threshold: Int = 1,
      size: Int = N
      ): Unit = {
    // Do it lazily, so we only run enough iterations to collect threshold number of successes
    val successes = (0 until runs)
        .toStream
        .map { x: Int =>
    // System.gc() // Might decrease odds of GC happening later, but way slower
      val (iActual, nActual, tstream) = countingTSeq(size)
      op(tstream)
      if (debug) println(iActual.get -> nActual.get)
      iActual.get -> nActual.get
    }.filter { case (iAct: Long, nAct: Long) => iAct == i && nAct == n }
        .take(threshold)

    assert(successes.length >= threshold)
  }
}
