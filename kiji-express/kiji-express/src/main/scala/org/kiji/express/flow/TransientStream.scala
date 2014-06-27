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

import scala.ref.WeakReference

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * `TransientStream` is a special case of [[scala.collection.Seq]] meant to be backed by a source
 * which is larger than memory. The `TransientStream` constructor takes a function which generates
 * iterators over the backing collection. When any `Seq` operation is called on a `TransientStream`,
 * the `TransientStream` will request an iterator from the generator, create a stream from the
 * iterator, and proxy the call through to the new stream. Finally, the created stream is cached in
 * a weak reference so any subsequent `Seq` operations on the `TransientStream` do not require
 * creating a new iterator over the backing collection, as long as the garbage collector has not
 * claimed the cached stream.
 *
 * As a result of using a weak reference cache, it is safe to hold a reference to a
 * `TransientStream` and realize all values, for example by using `foreach`.  This is not possible
 * with a [[scala.collection.immutable.Stream]], because holding the `head` of a `Stream` will cause
 * it not to be garbage collected.  Note, however, that operations on `TransientStream` never return
 * another `TransientStream`, instead a `Stream` is returned, with all the normal stream caveats
 * attached.
 *
 * {{
 *    val tstream = new TransientStream(...)
 *
 *    tstream.foreach { x => println(x) } // OK! if the collection is larger than memory,
 *                                        // the cached stream will be automatically GC'd
 *
 *    val stream = tstream.toStream // also could use tail, or any method resulting in a new seq
 *    stream.foreach { x => println(x) } // Danger! Will potentially result in an OutOfMemoryError,
 *                                       // because 'stream' holds a reference to the head
 * }}
 *
 * Finally, a `TransientStream` may only be treated as immutable if the backing collection from
 * which the iterator generator is drawing from is immutable. A `TransientStream` does not allow
 * mutating the underlying collection, but if the collection is mutated by another actor, then the
 * `TransientStream` may see the modifications, and thus referential transparency will be broken.
 *
 * Note that when a TransientStream is serialized, the genItr function and associated closures
 * are serialized.  A deserialized TransientStream will not have any cached state from the one
 * that was originally serialized.
 *
 * @tparam T type of contained elements.
 * @param genItr function produces a new iterator over the underlying collection of elements.
 */
@ApiAudience.Framework
@ApiStability.Stable
class TransientStream[T](val genItr: () => Iterator[T]) extends Stream[T] {

  @volatile @transient private var streamCache: WeakReference[Stream[T]] = new WeakReference(null)

  override def toStream: Stream[T] =
    if (null == streamCache) {
      this.synchronized {
        if (null == streamCache) {
          val newStream = genItr().toStream
          streamCache = new WeakReference(newStream)
          newStream
        } else {
          streamCache.get.getOrElse {
            val newStream = genItr().toStream
            streamCache = new WeakReference(newStream)
            newStream
          }
        }
      }
    } else {
      streamCache.get.getOrElse {
        this.synchronized {
          streamCache.get.getOrElse {
            val newStream = genItr().toStream
            streamCache = new WeakReference(newStream)
            newStream
          }
        }
      }
    }

  override protected def tailDefined: Boolean = streamCache.get.isDefined

  override def head: T = toStream.head

  override def tail: Stream[T] = toStream.tail

  override def force: Stream[T] = toStream.force

  override def hasDefiniteSize: Boolean = streamCache.get.isDefined && toStream.hasDefiniteSize

  override def isEmpty: Boolean = toStream.isEmpty

  override def toString: String =
    Option(streamCache)
        .map(s => "Transient" + s.toString)
        .getOrElse("TransientStream(?)")
}
