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

package org.kiji.modeling

import org.scalatest.FunSuite

class TestEvaluator extends Evaluator {
  override val distanceFn = DistanceFn(('c1, 'c2) -> 'count) {
    columns: (Double, Double) => {
      scala.math.abs(columns._1 - columns._2)
    }
  }
  override val aggregationFn = Some(AggregationFn('count -> 'totalCount)(0.0) {
    (countSoFar: Double, count: Double) => countSoFar + count
  } {
    total: Double => total
  })
}

class TestMalformedDistEvaluator extends Evaluator {
  override val distanceFn = DistanceFn(('c1, 'c2) -> 'count) {
    column: Double => column
  }
  override val aggregationFn = None
}

class TestMultipleReturnEvaluator extends Evaluator {
  override val distanceFn = DistanceFn('c1 -> ('count, 'otherCount)) {
    column: Int => (column, column + 1)
  }
}

class TestMalformedAggregatorEvaluator extends Evaluator {
  override val distanceFn = DistanceFn(('c1) -> 'count) {
    column: Double => column
  }
  override val aggregationFn = Some(AggregationFn(('c1, 'c2) -> 'totalCount)(0.0) {
    (countSoFar : Double, count : Double) => countSoFar + count
  } {
    total: Double => total
  })
}

class EvaluatorSuite extends FunSuite {
  test("An evaluator can be constructed via reflection") {
    val actual: Evaluator = classOf[TestEvaluator].newInstance
    val expected: Evaluator = new TestEvaluator

    val actualDistFields = actual
        .distanceFn
        .fields
    val expectedDistFields = expected
        .distanceFn
        .fields
    assert(expectedDistFields === actualDistFields)

    val actualAggrFields = actual
        .aggregationFn
        .get
        .fields
    val expectedAggrFields = expected
        .aggregationFn
        .get
        .fields
    assert(expectedAggrFields === actualAggrFields)

    val actualDistFn: Tuple2[Double, Double] => Double = actual
        .distanceFn
        .fn
        .asInstanceOf[Tuple2[Double, Double] => Double]
    val expectedDistFn: Tuple2[Double, Double] => Double = expected
        .distanceFn
        .fn
        .asInstanceOf[Tuple2[Double, Double] => Double]
    assert(expectedDistFn(1.0, 1.0) === actualDistFn(1.0, 1.0))

    val actualAggrFoldFn: (Double, Double) => Double = actual
        .aggregationFn
        .get
        .foldFn
        .asInstanceOf[(Double, Double) => Double]
    val expectedAggrFoldFn: (Double, Double) => Double = expected
        .aggregationFn
        .get
        .foldFn
        .asInstanceOf[(Double, Double) => Double]
    assert(expectedAggrFoldFn(1.0, 1.0) === actualAggrFoldFn(1.0, 1.0))

    val actualAggrMapFn: Double => Double = actual
        .aggregationFn
        .get
        .mapFn
        .asInstanceOf[Double => Double]
    val expectedAggrMapFn: Double => Double = expected
        .aggregationFn
        .get
        .mapFn
        .asInstanceOf[Double => Double]
    assert(expectedAggrMapFn(1.0) === actualAggrMapFn(1.0))
  }

  test("An evaluator with invalid field selection on DistanceFn will fail on construction") {
    intercept[AssertionError] {
      new TestMalformedDistEvaluator
    }
  }

  test("An evaluator with multiple return values works correctly") {
    val evaluator: Evaluator = new TestMultipleReturnEvaluator

    val fn: Int => (Int, Int) = evaluator
        .distanceFn
        .fn
        .asInstanceOf[Int => (Int, Int)]
    assert((2, 3) === fn(2))
  }

  test("An evaluator with invalid field selection on AggregationFn will fail on construction") {
    intercept[AssertionError] {
      new TestMalformedAggregatorEvaluator
    }
  }
}
