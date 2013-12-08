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
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.util.Tuples
import org.kiji.modeling.ExtractFn
import org.kiji.modeling.Extractor

/**
 * Base class for prebuilt extractors for performing simple selection tasks from a slice of a Kiji
 * table. Currently three simple selection extractors are provided:
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
    val selectFn: Seq[FlowCell[Any]] => R)
    extends Extractor {
  override val extractFn: ExtractFn[Any, R] = extract(Fields.ALL -> Fields.RESULTS) {
    case tuple: Product => {
      val returnValues: Seq[R] = tuple
          .productIterator
          .asInstanceOf[Iterator[Seq[FlowCell[Any]]]]
          .map { slice: Seq[FlowCell[Any]] => selectFn(slice) }
          .toSeq

      Tuples.seqToTuple(returnValues)
    }
    case slice: Seq[FlowCell[_]] => selectFn(slice.asInstanceOf[Seq[FlowCell[Any]]])
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
private object FirstValueExtractor {
  def selectFirstValue(slice: Seq[FlowCell[Any]]): Any = slice.head.datum
}

/**
 * Selects the last value of all slices required for the extract phase of a model.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final class LastValueExtractor
    extends SelectorExtractor[Any](LastValueExtractor.selectLastValue)
private object LastValueExtractor {
  def selectLastValue(slice: Seq[FlowCell[Any]]): Any = slice.last.datum
}

/**
 * Selects all values of all slices required for the extract phase of a model. Values from each
 * slice will be placed into a sequence.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final class SliceExtractor
    extends SelectorExtractor[Iterable[Any]](SliceExtractor.selectSlice)
private object SliceExtractor {
  def selectSlice(slice: Seq[FlowCell[Any]]): Iterable[Any] = {
    slice
        .map { cell => cell.datum }
  }
}
