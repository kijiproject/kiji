/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.testing.fakehtable

import java.util.{NavigableMap => JNavigableMap}

/**
 * Wraps a Java iterator on a TreeMap into a Scala iterator.
 *
 * JavaConverters.mapAsScalaMap does not work on NavigableMap, as it uses a HashMap internally,
 * and this breaks the ordering while iterating on the map.
 */
class JNavigableMapIterator[A, B] (
    val underlying: JNavigableMap[A, B]
) extends Iterator[(A, B)] {
  private val it = underlying.entrySet.iterator

  override def hasNext: Boolean = {
    return it.hasNext
  }

  override def next(): (A, B) = {
    val entry = it.next
    return (entry.getKey, entry.getValue)
  }

  override def hasDefiniteSize: Boolean = {
    return true
  }
}

/** Implicitly created wrapper to add JNavigableMap.asScalaIterator(). */
class JNavigableMapWithAsScalaIterator[A, B](
    val underlying: JNavigableMap[A, B]
) {
  def asScalaIterator(): Iterator[(A, B)] = {
    return new JNavigableMapIterator(underlying)
  }
}

object JNavigableMapWithAsScalaIterator {
  /** Adds JNavigableMap.asScalaIterator(). */
  implicit def javaNavigableMapAsScalaIterator[A, B](
      jmap: JNavigableMap[A, B]
  ): JNavigableMapWithAsScalaIterator[A, B] = {
    return new JNavigableMapWithAsScalaIterator(underlying = jmap)
  }
}
