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

package org.kiji.express

import scala.annotation.implicitNotFound
import scala.math.Numeric

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KijiCell

/**
 * A collection of [[org.kiji.express.Cell]]s that can be grouped and ordered as needed. Cells are
 * the smallest unit of information in a Kiji table; each cell contains a datum (as well as column
 * family, qualifier, and version.) Slices are initially ordered first by qualifier and then reverse
 * chronologically (latest first) by version.
 *
 * ===Ordering===
 * The order of the underlying cells can be modified arbitrarily, but we provide convenience methods
 * for common use cases.
 *
 * To order a KijiSlice chronologically, you may write {{{
 * chronological: KijiSlice = slice.orderChronologically()
 * }}}.
 * To order a KijiSlice reverse chronologically, you may write {{{
 * reverseChronological: KijiSlice =slice.orderReverseChronologically()
 * }}}.
 * To order a KijiSlice by column qualifier, you may write {{{
 * qualifier: KijiSlice = slice.orderByQualifier()
 * }}}.
 *
 *
 * ===Grouping===
 * KijiSlices can be grouped together by arbitrary criteria, but we provide a convenience method for
 * a common case.
 *
 * To group KijiSlices by column qualifier, you may write
 * {{{
 * groupedSlices: Map[String, KijiSlice[T]] = slice.groupByQualifier()
 * }}}.
 *
 * Slices can also be arbitrarily grouped by passing in a discriminator function, that defines the
 * grouping criteria, to groupBy().
 *
 * Accessing Values:
 * The underlying collection of cells can be obtained by {{{
 * myCells: Seq[Cell] = mySlice.cells
 * }}}.
 * This Sequence will respect the ordering of the KijiSlice.
 *
 *
 * @param cells A sequence of [[org.kiji.express.Cell]]s for a single entity.
 * @tparam T is the type of the data stored in the underlying cells.
 */
@ApiAudience.Public
@ApiStability.Experimental
class KijiSlice[T] private[express] (val cells: Seq[Cell[T]]) {
  /**
   * Gets the first cell, as decided by the ordering of the slice.
   *
   * @return the first cell in the collection, as determined by the sorted order.
   */
  def getFirst(): Cell[T] = cells(0)

  /**
   * Gets the value of the first cell, as decided by the ordering of the slice.
   *
   * @return the first value in the collection, as determined by sorted order.
   */
  def getFirstValue(): T = getFirst().datum

  /**
   * Gets the last cell, as decided by the ordering of the slice.
   *
   * @return the last cell in the collection, as determined by the sorted order.*/
  def getLast(): Cell[T] = cells(cells.length - 1)

  /**
   * Gets the value of the last cell, as decided by the ordering of the slice.
   *
   * @return the last value in the collection, as determined by sorted order.
   */
  def getLastValue(): T = getLast().datum

  /**
   * Generates a new KijiSlice that is ordered according to the [[scala.math.Ordering]] passed in.
   *
   * @param ordering the ordering to sort the slice by.
   * @return A reordered KijiSlice.
   */
  def orderBy(ordering: Ordering[Cell[T]]): KijiSlice[T] = {
    val orderedCells: Seq[Cell[T]] = cells.sorted(ordering)
    new KijiSlice[T] (orderedCells)
  }

  /**
   * Generates a new KijiSlice that is ordered chronologically (oldest to newest) by version number
   * of the underlying cells.
   *
   * @return a new KijiSlice that is ordered chronologically by version number of the underlying
   *    cells.
   */
  def orderChronologically(): KijiSlice[T] = orderBy(Ordering.by {cell: Cell[T] => cell.version} )

  /**
   * Generates a new KijiSlice that is ordered reverse chronologically (newest to oldest) by version
   * number of the underlying cells.
   *
   * @return a new KijiSlice that is ordered reverse chronologically by version number of the
   *    underlying cells.
   */
  def orderReverseChronologically(): KijiSlice[T] = orderBy(Ordering.by {cell: Cell[T] =>
      cell.version}.reverse)

  /**
   * Generates a new KijiSlice that is ordered alphabetically by qualifier of the underlying cells.
   *
   * @return a new KijiSlice that is ordered alphabetically by qualifier of the underlying cells.
   */
  def orderByQualifier(): KijiSlice[T] = orderBy(Ordering.by {cell: Cell[T] => cell.qualifier} )

  /**
   * Partitions this KijiSlice into a map from keys to KijiSlices according to some discriminator
   * function.
   *
   * @param fn a discriminator function.
   * @tparam K is the type of the key returned by the discriminator function.
   * @return a map from keys to KijiSlices, such that every cell that gets mapped to the same key by
   *    the discriminator function is in the same KijiSlice.
   */
  def groupBy[K](fn: (Cell[T] => K)) :Map[K, KijiSlice[T]] = {
    val pairs: Map[K, Seq[Cell[T]]] = cells.groupBy(fn)
    val makeNewSlice: (((K, Seq[Cell[T]])) => (K, KijiSlice[T])) =
    { case (key, valueCells) =>
      (key, new KijiSlice[T](valueCells))
    }
    pairs.map(makeNewSlice)
  }

  /**
   * Partitions this KijiSlice into a map from qualifiers to KijiSlices associated with cells of the
   * given qualifier.
   *
   * @return a map from keys to KijiSlices, such that every cell that gets mapped to the same key
   *    has the same qualifier.
   */
  def groupByQualifier(): Map[String, KijiSlice[T]] = groupBy[String]({cell: Cell[T] =>
      cell.qualifier})

  /**
   * Gets the number of underlying [[org.kiji.express.Cell]]s.
   *
   * @return the number of underlying [[org.kiji.express.Cell]]s.
   */
  val size: Int = cells.size

  /**
   * Gets the value of the underlying [[org.kiji.express.Cell]] as a Double
   *
   * @param cell whose value to return as a Double
   * @return cell's value as a Double
   */
  private def getValueAsDouble(cell: Cell[T])(implicit num: Numeric[T]): Double = {
    num.toDouble(cell.datum)
  }

  /**
   * Returns the sum of values, specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @param fn retrieves the value used for summation
   * @return the sum of values, specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def sum(fn: (Cell[T] => Double)): Double = {
    cells.foldLeft(0.0) { (sum, cell) =>
      sum + fn(cell)
    }
  }

  /**
   * Returns the sum of values, assuming the value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the sum of values, assuming the value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
                  + "accepts a function argument.")
  def sum(implicit num: Numeric[T]): Double = sum { cell: Cell[T] => getValueAsDouble(cell)(num) }

  /**
   * Returns the squared sum of values, specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @param fn retrieves the value used for summation of squares.
   * @return the squared sum of values, specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def sumSquared(fn: (Cell[T] => Double)): Double = {
    cells.foldLeft(0.0) { (sum, cell) =>
      val cellVal = fn(cell)
      sum + (cellVal * cellVal)
    }
  }

  /**
   * Returns the squared sum of values, assuming the value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the squared sum of values, assuming the value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
                  + "accepts a function argument.")
  def sumSquared(implicit num: Numeric[T]): Double = {
    sumSquared { cell: Cell[T] => getValueAsDouble(cell)(num) }
  }

  /**
   * Returns the average value, specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @param fn retrieves the value used for calculating the average
   * @return the average value, specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def avg(fn: (Cell[T] => Double)): Double = {
    val totalVal = sum(fn)
    return totalVal / size
  }

  /**
   * Returns the average value, assuming the value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the average value, assuming the value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
                  + "accepts a function argument.")
  def avg(implicit num: Numeric[T]): Double = avg { cell: Cell[T] => getValueAsDouble(cell)(num) }

  /**
   * Returns the variance, where each value is specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @param fn retrieves the value used for calculating the variance
   * @return the variance, where each value is specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def variance(fn: (Cell[T] => Double)): Double = {
    val sumAndSumOfSquares = cells.foldLeft((0.0,0.0)) { (sumTuple, cell) =>
      //The tuple stores the sum(_1) and sumSquared(_2) for later use in the calculation
      //of the variance.
      val cellVal = fn(cell)
      (sumTuple._1 + cellVal, sumTuple._2 + (cellVal * cellVal))
    }

    val sumOfSquares = sumAndSumOfSquares._2
    val sum = sumAndSumOfSquares._1
    val numCells = size
    val avgVal = sum / size

    return (sumOfSquares / numCells) - (avgVal * avgVal)
  }

  /**
   * Returns the variance, assuming each cell's value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the variance, assuming each cell's value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
                  + "accepts a function argument.")
  def variance(implicit num: Numeric[T]): Double = {
    variance { cell: Cell[T] => getValueAsDouble(cell)(num) }
  }

  /**
   * Returns the standard deviation, where each value is specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @param fn retrieves the value used for calculating the variance
   * @return the standard deviation, where each value is specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def stddev(fn: (Cell[T] => Double)): Double = {
    return scala.math.sqrt(variance(fn)).toDouble
  }

  /**
   * Returns the standard deviation, assuming each cell's value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the standard deviation, assuming each cell's value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
                  + "accepts a function argument.")
  def stddev(implicit num: Numeric[T]): Double = {
    stddev { cell: Cell[T] => getValueAsDouble(cell)(num) }
  }

  /**
   * Returns the minimum value, specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @param fn retrieves the value used for calculating the variance
   * @return the minimum value, specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def min(fn: (Cell[T] => Double)): Double = {
    cells.foldLeft(Double.MaxValue) { (currentValue: Double, currentCell: Cell[T]) =>
      val cellValue = fn(currentCell)
      if(cellValue < currentValue) {
        cellValue
      } else {
        currentValue
      }
    }
  }

  /**
   * Returns the minimum value,
   * of the KijiSlice's underlying [[org.kiji.express.Cell]]s assuming the Cell's value is numeric.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the minimum value, specified by the retrieval function,
   *    from the [[org.kiji.express.Cell]]s of the underlying the KijiSlice
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
                  + "accepts a function argument.")
  def min(implicit num: Numeric[T]): Double = {
    min { cell: Cell[T] => getValueAsDouble(cell)(num) }
  }

  /**
   * Returns the maximum value, specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @param fn retrieves the value used for calculating the variance
   * @return the maximum value, specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def max(fn: (Cell[T] => Double)): Double = {
    cells.foldLeft(Double.MinValue) { (currentValue: Double, currentCell: Cell[T]) =>
      val cellValue = fn(currentCell)
      if(cellValue > currentValue) {
        cellValue
      } else {
        currentValue
      }
    }
  }

  /**
   * Returns the maximum value,
   * of the KijiSlice's underlying [[org.kiji.express.Cell]]s assuming the Cell's value is numeric.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the maximum value, specified by the retrieval function,
   *    from the [[org.kiji.express.Cell]]s of the underlying the KijiSlice
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
                  + "accepts a function argument.")
  def max(implicit num: Numeric[T]): Double = {
    max { cell: Cell[T] => getValueAsDouble(cell)(num) }
  }

  override def equals(otherSlice: Any): Boolean = otherSlice match {
    case otherSlice: KijiSlice[_] => (otherSlice.cells == cells)
    case _ => false
  }

  override def hashCode(): Int = {
    cells.hashCode()
  }

  override def toString(): String = {
    "KijiSlice: %s".format(cells.toString)
  }
}

/**
 * A factory for KijiSlices.
 */
object KijiSlice {
 /**
  * A factory method for instantiating a KijiSlice, given an iterator of
  * [[org.kiji.schema.KijiCell]]s.
  * @param cellIter an iterator over KijiCells to instantiate the slice with.
  * @tparam T is the type of the data stored in the underlying cells.
  * @return a KijiSlice that contains the data passed in through the cellIter.
  */
  private[express] def apply[T](cellIter: Iterator[KijiCell[T]]): KijiSlice[T] = {
    val cells: Seq[Cell[T]] = cellIter.toSeq.map { kijiCell: KijiCell[T] => Cell[T](kijiCell) }
    new KijiSlice[T](cells)
  }
}
