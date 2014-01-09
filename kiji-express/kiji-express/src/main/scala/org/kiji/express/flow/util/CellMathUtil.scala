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

package org.kiji.express.flow.util

import scala.annotation.implicitNotFound

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.FlowCell

/**
 * Provides aggregator functions for sequences of [[org.kiji.express.flow.FlowCell]]s.
 */
@ApiAudience.Private
@ApiStability.Stable
object CellMathUtil {
  private val logger: Logger = LoggerFactory.getLogger(CellMathUtil.getClass)

  /**
   * Computes the sum of the values stored within the [[org.kiji.express.flow.FlowCell]]s in the
   * provided `Seq`.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within this collection
   * contain values of a type that has an implicit implementation of the `scala.Numeric` trait.
   *
   * @tparam T is the type of the [[org.kiji.express.flow.FlowCell]]s contained in the slice.
   * @param slice is the collection of [[org.kiji.express.flow.FlowCell]]s to sum.
   * @return the sum of the values within the [[org.kiji.express.flow.FlowCell]]s in the provided
   *     slice.
   */
  @implicitNotFound("The type of data contained within the provided cells does not support" +
      " numeric operations through the scala.Numeric trait.")
  def sum[T](slice: Seq[FlowCell[T]])(implicit num: Numeric[T]): T = {
    slice.foldLeft(num.zero) { (sum: T, cell: FlowCell[T]) =>
      num.plus(sum, cell.datum)
    }
  }

  /**
   * Computes the mean of the values stored within the [[org.kiji.express.flow.FlowCell]]s in the
   * provided slice.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within this collection
   * contain values of a type that has an implicit implementation of the `scala.Numeric` trait.
   *
   * @tparam T is the type of the [[org.kiji.express.flow.FlowCell]]s contained in the slice.
   * @param slice is the collection of [[org.kiji.express.flow.FlowCell]]s to compute the mean of.
   * @return the mean of the values within the provided [[org.kiji.express.flow.FlowCell]]s.
   */
  @implicitNotFound("The type of data contained within the provided cells does not support" +
      " numeric operations through the scala.Numeric trait.")
  def mean[T](slice: Seq[FlowCell[T]])(implicit num: Numeric[T]): Double = {
    val n = slice.size
    slice.foldLeft(0.0) { (mean: Double, cell: FlowCell[T]) =>
      mean + (num.toDouble(cell.datum) / n)
    }
  }

  /**
   * Finds the minimum of the values stored within the [[org.kiji.express.flow.FlowCell]]s in the
   * provided slice.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within this collection
   * contain values of a type that has an implicit implementation of the `scala.Ordering` trait.
   *
   * @tparam T is the type of the [[org.kiji.express.flow.FlowCell]]s contained in the slice.
   * @param slice is the collection of [[org.kiji.express.flow.FlowCell]]s to find the minimum of.
   * @return the minimum value contained in the `Seq` of [[org.kiji.express.flow.FlowCell]]s.
   */
  @implicitNotFound("The type of data contained within the provided cells does not support" +
      " ordering operations through the scala.Ordering trait.")
  def min[T](slice: Seq[FlowCell[T]])(implicit cmp: Ordering[T]): T = {
    slice.min(Ordering.by { cell: FlowCell[T] => cell.datum }).datum
  }

  /**
   * Finds the maximum of the values stored within the [[org.kiji.express.flow.FlowCell]]s in the
   * provided slice.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within this collection
   * contain values of a type that has an implicit implementation of the `scala.Ordering` trait.
   *
   * @tparam T is the type of the [[org.kiji.express.flow.FlowCell]]s contained in the slice.
   * @param slice is the collection of [[org.kiji.express.flow.FlowCell]]s to find the maximum of.
   * @return the maximum of the value within the provided [[org.kiji.express.flow.FlowCell]]s.
   */
  @implicitNotFound("The type of data contained within the provided cells does not support" +
      " ordering operations through the scala.Ordering trait.")
  def max[T](slice: Seq[FlowCell[T]])(implicit cmp: Ordering[T]): T = {
    slice.max(Ordering.by { cell: FlowCell[T] => cell.datum }).datum
  }

  /**
   * Computes the standard deviation of the values stored within the
   * [[org.kiji.express.flow.FlowCell]]s in the provided slice.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within this collection
   * contain values of a type that has an implicit implementation of the `scala.Numeric` trait.
   *
   * @tparam T is the type of the [[org.kiji.express.flow.FlowCell]]s contained in the slice.
   * @param slice the `Seq` of [[org.kiji.express.flow.FlowCell]]s to compute the standard
   *     deviation of.
   * @return the standard deviation of the values within the [[org.kiji.express.flow.FlowCell]]s in
   *     the provided slice.
   */
  @implicitNotFound("The type of data contained within the provided cells does not support" +
      " numeric operations through the scala.Numeric trait.")
  def stddev[T](slice:Seq[FlowCell[T]])(implicit num: Numeric[T]): Double = {
    scala.math.sqrt(variance(slice))
  }

  /**
   * Computes the squared sum of the values stored within the [[org.kiji.express.flow.FlowCell]]s in
   * the provided slice.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within this collection
   * contain values of a type that has an implicit implementation of the `scala.Numeric` trait.
   *
   * @tparam T the type of the [[org.kiji.express.flow.FlowCell]]s contained in the slice.
   * @param slice the collection of [[org.kiji.express.flow.FlowCell]]s to compute the squared
   *     sum of.
   * @return the squared sum of the values in the provided [[org.kiji.express.flow.FlowCell]]s.
   */
  @implicitNotFound("The type of data contained within the provided cells does not support" +
      " numeric operations through the scala.Numeric trait.")
  def sumSquares[T](slice: Seq[FlowCell[T]])(implicit num: Numeric[T]): T = {
    slice.foldLeft(num.zero) { (sumSquares: T, cell: FlowCell[T]) =>
      num.plus(sumSquares, num.times(cell.datum, cell.datum))
    }
  }

  /**
   * Computes the variance of the values stored within the [[org.kiji.express.flow.FlowCell]]s in
   * the provided slice.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within this collection
   * contain values of a type that has an implicit implementation of the `scala.Numeric` trait.
   *
   * @tparam T is the type of the [[org.kiji.express.flow.FlowCell]]s contained in the slice.
   * @param slice is the collection of [[org.kiji.express.flow.FlowCell]]s to compute the
   *     variance of.
   * @return the variance of the values within the [[org.kiji.express.flow.FlowCell]]s in the
   *     provided slice.
   */
  @implicitNotFound("The type of data contained within the provided cells does not support" +
      " numeric operations through the scala.Numeric trait.")
  def variance[T](slice: Seq[FlowCell[T]])(implicit num: Numeric[T]): Double = {
    val (n, _, m2) = slice
        .foldLeft((0L, 0.0, 0.0)) { (acc: (Long, Double, Double), cell: FlowCell[T]) =>
          val (n, mean, m2) = acc
          logger.debug("cell: %s".format(cell.datum))
          logger.debug("acc: %s".format(acc))
          val x = num.toDouble(cell.datum)

          val nPrime = n + 1
          val delta = x - mean
          val meanPrime = mean + delta / nPrime
          val m2Prime = m2 + delta * (x - meanPrime)

          (nPrime, meanPrime, m2Prime)
        }

    m2 / n
  }
}
