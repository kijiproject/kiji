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

import java.util.{Map => JMap}
import java.lang.{Long => JLong}

import org.kiji.schema.ColumnVersionIterator
import org.kiji.schema.MapFamilyVersionIterator

/**
 * A collection of [[org.kiji.express.Cell]]s, retrieved in pages, that can be seamlessly iterated
 * over. Paging limits the amount of memory used, by limiting the number of cells retrieved at a
 * time. For more information on paging in general, see [[org.kiji.schema.KijiPager]].
 *
 * To use paging in an Express Flow, you specify the maximum number of cells you want in a page.
 * To request no more than 3 cells in memory at a
 * {{{
 *      KijiInput(args("input"))(Map(Column("family:column", all).withPaging(3) -> 'column))

 * }}}
 *
 * If paging is requested for a column in a Kiji table, the tuple field returned will contain a
 * [[org.kiji.express.PagedKijiSlice]].
 *
 * @param cells that can be iterated over for processing.
 * @tparam T is the type of the data contained in the underlying cells.
 */
case class PagedKijiSlice[T] private (cells: Iterator[Cell[T]]) extends Iterable[Cell[T]]
    with java.io.Serializable {
  override def iterator: Iterator[Cell[T]] = cells
}

/**
 * Companion object that provides factory methods for instantiating PagedKijiSlices.
 */
object PagedKijiSlice extends java.io.Serializable {
  private[express] def apply[T](family: String, qualifier: String,
      columnIterator: ColumnVersionIterator[T]): PagedKijiSlice[T] = {
    new PagedKijiSlice[T](new ColumnCellIterator[T](family, qualifier, columnIterator))
  }

  private[express] def apply[T](family: String, mapFamilyIterator: MapFamilyVersionIterator[T]):
      PagedKijiSlice[T] =  {
    new PagedKijiSlice[T](new MapFamilyCellIterator[T](family, mapFamilyIterator))
  }
}

/**
 * Thin wrapper class around the MapFamilyVersionIterator that allows us to lazily transform data
 * returned from a MapFamilyVersionIterator into a [[org.kiji.express.Cell]].
 *
 * @param family of the column we are iterating over.
 * @param mapFamilyIterator to back this iterator.
 * @tparam T is the type of the datum in the returned [[org.kiji.express.Cell]]'s
 */
private[express] class MapFamilyCellIterator[T]private[express](family: String,
    mapFamilyIterator: MapFamilyVersionIterator[T]) extends Iterator[Cell[T]] {
  override def hasNext: Boolean = mapFamilyIterator.hasNext

  override def next(): Cell[T] = {
    val entry: MapFamilyVersionIterator.Entry[T] = mapFamilyIterator.next()
    new Cell[T](family, entry.getQualifier, entry.getTimestamp, entry.getValue)
  }
}

/**
 * Thin wrapper class around the ColumnVersionIterator that allows us to lazily transform data
 * returned from a ColumnVersionIterator into a [[org.kiji.express.Cell]].
 *
 * @param family of the column we are iterating over.
 * @param qualifier of the column we are iterating over.
 * @param columnIterator to back this iterator.
 * @tparam T is the type of the datum in the returned [[org.kiji.express.Cell]]'s
  */
private[express] class ColumnCellIterator[T](family: String, qualifier: String,
    columnIterator: ColumnVersionIterator[T]) extends Iterator[Cell[T]] {
  override def hasNext: Boolean = columnIterator.hasNext

  override def next(): Cell[T] = {
    val entry: JMap.Entry[JLong,T] = columnIterator.next()
    new Cell[T](family, qualifier, entry.getKey, entry.getValue)
  }
}
