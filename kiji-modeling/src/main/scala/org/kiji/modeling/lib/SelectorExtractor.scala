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

package org.kiji.modeling.lib

import cascading.tuple.Fields

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.KijiSlice
import org.kiji.modeling.Extractor
import org.kiji.express.util.Tuples

/**
 * Base class for prebuilt extractors for performing simple selection tasks from a
 * [[org.kiji.express.KijiSlice]]. Currently three simple selection extractors are provided:
 * <ul>
 *   <li>[[org.kiji.modeling.lib.FirstValueExtractor]] -
 *       Selects the first value of a slice.</li>
 *   <li>[[org.kiji.modeling.lib.LastValueExtractor]] -
 *       Selects the last value of a slice.</li>
 *   <li>[[org.kiji.modeling.lib.SliceExtractor]] -
 *       Selects the entire slice and puts each cell into a sequence.</li>
 * </ul>
 *
 * @tparam R is the return type of the selection.
 * @param selectFn defining the expected selection behavior.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
sealed abstract class SelectorExtractor[R](
    val selectFn: KijiSlice[Any] => R)
    extends Extractor {
  override val extractFn = extract(Fields.ALL -> Fields.RESULTS) { data: Any =>
    data match {
      case tuple: Product => {
        val returnValues: Seq[R] = tuple
            .productIterator
            .toSeq
            .asInstanceOf[Seq[KijiSlice[Any]]]
            .map { slice: KijiSlice[Any] => selectFn(slice) }

        Tuples.seqToTuple(returnValues)
      }
      case slice: KijiSlice[_] => selectFn(slice.asInstanceOf[KijiSlice[Any]])
    }
  }
}

/**
 * Selects the first value of all slices required for the extract phase of a model.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final class FirstValueExtractor
    extends SelectorExtractor[Any](FirstValueExtractor.selectFirstValue)
private[modeling] object FirstValueExtractor {
  def selectFirstValue(slice: KijiSlice[Any]): Any = slice.getFirstValue
}

/**
 * Selects the last value of all slices required for the extract phase of a model.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final class LastValueExtractor
    extends SelectorExtractor[Any](LastValueExtractor.selectLastValue)
private[modeling] object LastValueExtractor {
  def selectLastValue(slice: KijiSlice[Any]): Any = slice.getLastValue
}

/**
 * Selects all values of all slices required for the extract phase of a model. Values from each
 * slice will be placed into a sequence.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final class SliceExtractor
    extends SelectorExtractor[Seq[Any]](SliceExtractor.selectSlice)
private[modeling] object SliceExtractor {
  def selectSlice(slice: KijiSlice[Any]): Seq[Any] = {
    slice
        .cells
        .map { cell => cell.datum }
  }
}
