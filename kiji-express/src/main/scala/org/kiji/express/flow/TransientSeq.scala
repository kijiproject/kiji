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

import scala.collection.{mutable, SeqView}

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import scala.collection.immutable.Stream.StreamBuilder

/**
 * `TransientSeq` is a special case of [[scala.collection.Seq]] meant to be backed by a source which
 * is potentially larger than memory or mutable. The `TransientSeq` constructor takes an iterator
 * generator- a no arg function which returns an iterator over the backing collection.
 * `TransientSeq` is a [[scala.collection.SeqView]], and thus it applies all transformations lazily.
 * Furthermore, when `force` is called on a `TransientSeq`, an iterator is created, and a lazily
 * evaluated Stream is returned. Operations which truly force a `TransientSeq` into evaluation
 * (and thus creating and using an iterator on the backing collection), include `head`, `tail`,
 * `foreach`, `apply`, `length`, `size`, `contains`, `diff`, `union`, `intersect`, and `sorted`.
 *
 * From a user's perspective, a `TransientSeq` is not mutable, but the underlying collection may be
 * mutable. [[scala.collection.mutable]] mutator methods are not provided, thus users cannot mutate
 * a `TransientSeq`. If the collection underlying the iterator generator is immutable (that is, the
 * iterator generator always returns iterators containing the same elements in the same order), then
 * the `TransientSeq` can be treated as immutable.  If the backing collection is not immutable, then
 * `TransientSeq` will not be immutable, because it will not guarantee referential transparency.
 *
 * It is important when using a `TransientSeq` to limit the calls to operations which create and use
 * an iterator on the backing collection.  For instance:
 * {{
 *    val tseq = new TransientSeq(...)
 *
 *    // Slow! creates 3 separate iterators over the backing collection
 *    val first = tseq(0)
 *    val second = tseq(1)
 *    val third = tseq(2)
 *
 *    // Better, only creates a single iterator over the backing collection
 *    val elements = tseq.take(3).toList // forces first 3 elements of transient representation
 *                                       // to in-memory list
 *    val first = elements(0)
 *    val second = elements(1)
 *    val third = elements(2)
 * }}
 *
 * Be careful when using `toList` to force the `TransientSeq` to an in-memory list.  This could
 * cause an [[java.lang.OutOfMemoryError]] if the items in the `TransientSeq` do not fit in the
 * VM's heap space. In this case, use filters, aggregation, take, or drop to reduce the number of
 * elements.
 *
 * Additionally, `TransientSeq`s should not be passed to recursive methods, instead, `force` the
 * `TransientSeq` into a `Stream` and then pass it to the recursive method, otherwise a new
 * iterator will be created for every recursive call to `tail`.
 *
 * @param genItr function produces a new iterator of the underlying collection of elements.
 * @tparam T type of contained elements.
 */
@ApiAudience.Framework
@ApiStability.Experimental
class TransientSeq[+T](genItr: () => Iterator[T]) extends SeqView[T, Stream[T]] {
  require(genItr != null)

  override def length: Int = iterator.size

  override def apply(idx: Int): T = iterator.drop(idx).next()

  override def iterator: Iterator[T] = genItr()

  protected val underlying: Stream[T] = Stream()

  override def toString: String = "TransientSeq(...)"

  protected[this] override def newBuilder: mutable.Builder[T, SeqView[T, Stream[T]]] = new StreamBuilder[T].mapResult(x => x.view)
}
