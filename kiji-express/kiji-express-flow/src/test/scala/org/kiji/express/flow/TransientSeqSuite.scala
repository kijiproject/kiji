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

import scala.collection.mutable

import org.kiji.express.KijiSuite

class TransientSeqSuite extends KijiSuite {

  test("Paging through a TransientSeq twice will generate two iterators.") {
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      List().iterator
    }

    val seq = new TransientSeq(genItr)
    for (_ <- seq) {}
    for (_ <- seq) {}
    assert(counter === 2)
  }

  test("TransientSeq will not reuse iterators when doing transformations.") {
    var counter = 0
    def genItr(): Iterator[Int] =
      new Iterator[Int] {
        val itr = (0 until 5).iterator
        def hasNext: Boolean = itr.hasNext

        def next(): Int = {
          counter += 1
          itr.next()
        }
      }

    val seq = new TransientSeq(genItr)
    for (_ <- seq) {}
    for (_ <- seq) {}
    assert(counter === 10)
  }

  test("TransientSeq does not hold onto the results of an iterator.") {
    // The only (sure) way to test this is to make sure an OOME doesn't occur
    val max = 250 * 1000 * 1000 // 1 GB of Ints
    def genItr(): Iterator[Int] = (1 to max).iterator

    val seq = new TransientSeq(genItr)
    // val seq = genItr().toStream // Control case.  Will crash

    // Make sure there is a side effect when running through the iterator so it won't get JIT'd away
    var counter: Int = 0
    for (i <- seq) { counter = i }
    assert(counter === max)
  }

  test("Chaining transformations on a TransientSeq does not require an iterator.") {
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      (1 to Int.MaxValue).iterator
    }

    val tseq = new TransientSeq(genItr)
        .map(x => x + 1)
        .map(x => x * x)
        .take(10)
        .drop(3)
        .filter(x => x % 2 == 0)

    assert(counter === 0)
  }

  // C(i, n) is a complexity measure of the number of iterators requested from the TransientSeq, i,
  // and the number of elements requested from all iterators, n.  N is the number of elements held
  // by the underlying iterators of the TransientSeq.

  /** Good. */
  test("force is C(1, 1)") { assertComplexity(1, 1, tseq => tseq.force) }

  /** Good. */
  test("head is C(1, 1)") { assertComplexity(1, 1, tseq => tseq.head) }

  /** Poor. See below. */
  test("tail is C(1, 0)") { assertComplexity(1, 0, tseq => tseq.tail) }

  /** This is egregious. */
  test("tail * m is C(m, 0 + 1 + ... + m - 1)") {
    assertComplexity(4, 6, tseq => tseq.tail.tail.tail.tail)
  }

  /** And this is even worse. */
  test("tail * m then force is C(m + 1, 0 + 1 + m - 1 + m + 1)") {
    assertComplexity(5, 11, tseq => tseq.tail.tail.tail.tail.force)
  }

  /** Good. */
  test("force then tail * m is C(1, m + 1)") {
    assertComplexity(1, 5, tseq => tseq.force.tail.tail.tail.tail)
  }

  /** Good. */
  test("take(m) is C(0, 0)") { assertComplexity(0, 0, tseq => tseq.take(42)) }

  /** Good. */
  test("take(m) then force is C(1, 1)") { assertComplexity(1, 1, tseq => tseq.take(42).force) }

  /** Good. */
  test("drop(m) is C(0, 0)") { assertComplexity(0, 0, tseq => tseq.drop(42)) }

  /** Good. */
  test("drop(m) then force is C(1, m + 1)") { assertComplexity(1, 43, tseq => tseq.drop(42).force) }

  /** Good. */
  test("dropWhile is C(0, 0)") {
    assertComplexity(0, 0, tseq => tseq.dropWhile(x => x < 100))
  }

  /** Good. */
  test("dropWhile then force is C(1, O(N))") {
    assertComplexity(1, 100, tseq => tseq.dropWhile(x => x < 100).force)
  }

  /** Good. */
  test("immutable additions are C(0, 0)") {
    assertComplexity(0, 0, tseq => tseq ++ List(1, 2, 3))
    assertComplexity(0, 0, tseq => tseq ++: List(1, 2, 3))
    assertComplexity(0, 0, tseq => tseq.+:(1))
    assertComplexity(0, 0, tseq => tseq.:+(1))
  }

  def sum(a: Int, b: Int): Int = a + b

  /** Good. */
  test("fold is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).foldLeft(0)(sum))
    assertComplexity(1, 10, tseq => tseq.take(10).foldRight(0)(sum))
    assertComplexity(1, 10, tseq => tseq.take(10)./:(0)(sum))
    assertComplexity(1, 10, tseq => tseq.take(10).:\(0)(sum))
    assertComplexity(1, 10, tseq => tseq.take(10).aggregate(0)(sum, sum))
  }

  /** Poor. Use fold over reduce where possible. */
  test("reduce left is C(2, N)") {
    assertComplexity(2, 10, tseq => tseq.take(10).reduce(sum))
    assertComplexity(2, 10, tseq => tseq.take(10).reduceLeft(sum))
  }

  /** OK, but unsafe because of memory usage. */
  test("reduce right is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).reduceRight(sum))
  }

  /** Poor. */
  test("reduce option adds 1 iterator access") {
    assertComplexity(3, 10, tseq => tseq.take(10).reduceLeftOption(sum))
    assertComplexity(2, 10, tseq => tseq.take(10).reduceRightOption(sum))
  }

  /** Good. */
  test("flatten is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).map(x => List(x)).flatten)
  }

  /** Good. */
  test("map is C(0, 0)") {
    assertComplexity(0, 0, tseq => tseq.map(x => x + 1))
    assertComplexity(0, 0, tseq => tseq.flatMap(x => List(x, x)))
    assertComplexity(0, 0, tseq => tseq.reverseMap(x => x + 1))
  }

  /** Good. */
  test("map then force is C(1, 1)") {
    assertComplexity(1, 1, tseq => tseq.map(x => x + 1).force)
    assertComplexity(1, 1, tseq => tseq.flatMap(x => List(x, x)).force)
  }

  /** OK, but unsafe because of memory usage. */
  test("reverseMap then force is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).reverseMap(x => x + 1).force)
  }

  /** Good. */
  test("map then toList is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).map(x => x + 1).toList)
    assertComplexity(1, 10, tseq => tseq.take(10).flatMap(x => List(x, x)).toList)
    assertComplexity(1, 10, tseq => tseq.take(10).reverseMap(x => x + 1).toList)
  }

  /** Good. */
  test("filter is C(0, 0)") {
    assertComplexity(0, 0, tseq => tseq.filter(x => x % 2 == 0))
    assertComplexity(0, 0, tseq => tseq.filterNot(x => x % 2 == 0))
    assertComplexity(0, 0, tseq => tseq.collect { case x if x % 2 == 0 => x })
  }

  /** Good. */
  test("filter then force is C(1, x) where x is the index of the first non-filtered element") {
    assertComplexity(1, 2, tseq => tseq.filter(x => x % 2 == 0).force)
    assertComplexity(1, 1, tseq => tseq.filterNot(x => x % 2 == 0).force)
  }

  /** Good. */
  test("filter then toList is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).filter(x => x % 2 == 0).toList)
    assertComplexity(1, 10, tseq => tseq.take(10).filterNot(x => x % 2 == 0).toList)
  }

  /** Good. */
  test("membership ops are C(1, O(N))") {
    assertComplexity(1, 10, tseq => tseq.collectFirst { case x if x == 10 => x })
    assertComplexity(1, 10, tseq => tseq.contains(10))
    assertComplexity(1, 10, tseq => tseq.indexOf(10))
    assertComplexity(1, 10, tseq => tseq.indexWhere(_ == 10))
    assertComplexity(1, 10, tseq => tseq.indexWhere(_ == 10, 3))
    assertComplexity(1, 10, tseq => tseq.exists(_ == 10))
    assertComplexity(1, 10, tseq => tseq.find(_ == 10))
  }

  /** Good. */
  test("size is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).size)
    assertComplexity(1, 10, tseq => tseq.take(10).length)
  }

  /** Good. */
  test("copy ops are C(1, L) where L is the number of elements to copy") {
    assertComplexity(1, 10, tseq => tseq.copyToArray(Array.ofDim[Int](10)))
    assertComplexity(1, 10, tseq => tseq.copyToArray(Array.ofDim[Int](10), 0))
    assertComplexity(1, 10, tseq => tseq.copyToArray(Array.ofDim[Int](10), 0, 10))
    assertComplexity(1, 10, tseq => tseq.take(10).copyToBuffer(mutable.Buffer[Int]()))
  }

  /** Poor. */
  test("force then updated is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).updated(2, 99))
  }

  /** Good. */
  test("corresponds is C(1, O(Min(N, L))) where L is the length of the other Seq") {
    assertComplexity(1, 5, tseq => tseq.corresponds(1 to 5)(_ equals _))
  }

  /** Poor. Not lazy, will not complete without the take. */
  test("combinations is C(2, 2 * N)") {
    assertComplexity(2, 20, tseq => tseq.take(10).combinations(3))
  }

  /** Poor. Not lazy, will not complete without the take. */
  test("permutations is C(2, N)") {
    assertComplexity(2, 10, tseq => tseq.take(10).permutations)
  }

  /** OK, but unsafe because of memory usage. */
  test("groupBy is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).groupBy(x => x % 4 == 0))
  }

  /** Good. */
  test("grouped is C(1, 0)") {
    assertComplexity(1, 0, tseq => tseq.grouped(2))
  }

  /** Good. */
  test("zip is C(0, 0)") {
    assertComplexity(0, 0, tseq => tseq.zip(1 to 100))
    assertComplexity(0, 0, tseq => tseq.zipAll(1 to 100, 99, 99))
    assertComplexity(0, 0, tseq => tseq.zipWithIndex)
  }

  /** Poor. Not lazy, will not complete without the take. */
  test("unzip is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).zipWithIndex.unzip)
  }

  /** OK, but unsafe because of memory usage. */
  test("sorted is C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).sorted)
  }

  /** OK, but unsafe because of memory usage. */
  test("set operations are C(1, N)") {
    assertComplexity(1, 10, tseq => tseq.take(10).union(List(1, 2, 3, 99)))
    assertComplexity(1, 10, tseq => tseq.take(10).intersect(List(1, 2, 3, 99)))
    assertComplexity(1, 10, tseq => tseq.take(10).diff(List(1, 2, 3, 99)))
  }

  /** Good. */
  test("patch is C(0, 0)") {
    assertComplexity(0, 0, tseq => tseq.take(12).patch(0, List(10), 10))
  }

  /** Good. */
  test("startsWith is C(1, _)") {
    assertComplexity(1, 1, tseq => tseq.startsWith(List(10)))
    assertComplexity(1, 4, tseq => tseq.startsWith(List(10), 3))
  }

  /** Poor. */
  test("endsWith is C(2, 2 * N)") {
    assertComplexity(2, 20, tseq => tseq.take(10).endsWith(List(10)))
  }

  /** Extremely poor. */
  test("some operations are unsupported.") {
    intercept[UnsupportedOperationException] {
      assertComplexity(1, 10, tseq => tseq.take(12).containsSlice(List(9, 10)))
    }
    intercept[UnsupportedOperationException] {
      assertComplexity(1, 10, tseq => tseq.take(10).distinct)
    }
    intercept[UnsupportedOperationException] {
      assertComplexity(1, 10, tseq => tseq.take(12).indexOfSlice(List(10)))
    }
    intercept[UnsupportedOperationException] {
      assertComplexity(1, 10, tseq => tseq.take(10).takeRight(2))
    }
    intercept[UnsupportedOperationException] {
      assertComplexity(1, 100, tseq => tseq.take(10).dropRight(2))
    }
    intercept[UnsupportedOperationException] {
      assertComplexity(1, 10, tseq => tseq.take(10).lastIndexOfSlice(List(10)))
    }
    intercept[UnsupportedOperationException] {
      assertComplexity(1, 10, tseq => tseq.take(12).lastIndexOfSlice(List(10), 10))
    }
  }

  test("unsupported operations are supported after force.") {
    assertComplexity(1, 10, tseq => tseq.force.take(12).containsSlice(List(9, 10)))
    assertComplexity(1, 10, tseq => tseq.force.take(10).distinct.toList)
    assertComplexity(1, 10, tseq => tseq.force.take(12).indexOfSlice(List(10)))
    assertComplexity(1, 10, tseq => tseq.force.take(10).takeRight(2))
    assertComplexity(1, 10, tseq => tseq.force.take(10).dropRight(2))
    assertComplexity(1, 10, tseq => tseq.force.take(10).lastIndexOfSlice(List(10)))
    assertComplexity(1, 10, tseq => tseq.force.take(10).lastIndexOfSlice(List(10), 10))
  }

  def countingItr(): (AtomicLong, AtomicLong, () => Iterator[Int]) = {
    val i = new AtomicLong() // number of iterators requested.
    val n = new AtomicLong() // number of elements requested across all iterators.
    def genItr(): Iterator[Int] = {
      i.incrementAndGet()
      new Iterator[Int] {
        val itr = (1 to Int.MaxValue).iterator
        def hasNext: Boolean = itr.hasNext
        def next(): Int = { n.incrementAndGet() ; itr.next() }
      }
    }
    (i, n , genItr)
  }

  def countingTSeq(): (AtomicLong, AtomicLong, TransientSeq[Int]) = {
    val (i, n, genItr) = countingItr()
    (i, n, new TransientSeq(genItr))
  }

  def assertComplexity(i: Long, n: Long, op: TransientSeq[Int] => Unit): Unit = {
    val (iActual, nActual, tseq) = countingTSeq()
    op(tseq)
    assert(i === iActual.get, "iterator count")
    assert(n === nActual.get, "element count")
  }
}
