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

import org.kiji.express.KijiSuite

class TransientSeqSuite extends KijiSuite {

  test("Holding a reference to a TransientSeq and iterating through all values completes.") {
    def genItr(): Iterator[Int] = (1 to Int.MaxValue).iterator
    val seq: TransientSeq[Int] = new TransientSeq(genItr)

    val itr: Iterator[Int] = seq.iterator
    while (itr.hasNext) itr.next
  }

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

  test("TransientSeq will not eagerly evaluate iterators.") {
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
    seq.head
    assert(counter === 1)
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

  test("Forcing a TransientSeq requires an iterator.") {
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      (1 to Int.MaxValue).iterator
    }

    val tseq = new TransientSeq(genItr)

    tseq.force
    assert(counter === 1)
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

  test("Calling tail on a TransientSeq requires an iterator.") {
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      (1 to Int.MaxValue).iterator
    }

    val tseq = new TransientSeq(genItr).tail

    assert(counter === 1)
  }

  test("Calling tail n times on a forced TransientSeq requires a single iterator.") {
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      (1 to Int.MaxValue).iterator
    }

    val tseq = new TransientSeq(genItr).force.tail.tail.tail.tail.tail

    assert(counter === 1)
  }

  test("Adding new elements to a TransientSeq does not require an iterator.") {
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      (1 to Int.MaxValue).iterator
    }

    var tseq: Seq[Int] = new TransientSeq(genItr)
    tseq = tseq ++ List(1, 2, 3)
    tseq = tseq ++: List(1, 2, 3)
    tseq = tseq.+:(1)
    tseq = tseq.:+(1)

    assert(counter === 0)
  }

  test("fold on a TransientSeq require a single pass on an Iterator.") {
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      (1 to Int.MaxValue).iterator
    }

    var tseq: Seq[Int] = new TransientSeq(genItr).take(10)
    assert(tseq./:(0)(_ + _) === 55)
    assert(tseq.:\(0)(_ + _) === 55)
    assert(tseq.foldLeft(0)(_ + _) === 55)
    assert(tseq.foldRight(0)(_ + _) === 55)

    assert(counter === 4)
  }

  test("reduce on a TransientSeq requires two iterators.") {
    // This seems counterintuitive.  I think the first iterator only grabs the first element in
    // order to start off the fold.  Therefore reduce should be avoided in preference to fold where
    // possible.
    var counter = 0
    def genItr(): Iterator[Int] = {
      counter += 1
      (1 to Int.MaxValue).iterator
    }

    var tseq: Seq[Int] = new TransientSeq(genItr).take(10)
    assert(tseq.reduce(_ + _) === 55)
    assert(tseq.reduceLeft(_ + _) === 55)
    assert(tseq.reduceRight(_ + _) === 55)
    assert(tseq.reduceLeftOption(_ + _) === Some(55))
    assert(tseq.reduceRightOption(_ + _) === Some(55))

   assert(counter === 10)
  }
}
