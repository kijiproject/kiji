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

package org.kiji.mapreduce.lib.avro;

import java.util.Iterator;

import org.apache.avro.mapred.AvroValue;

/**
 * A class that iterates over Avro messages by unwrapping the
 * AvroValue container object.  This is used to expose a cleaner
 * interface to clients, so they can iterate directly over the Avro
 * messages instead of the AvroValue wrapper object.
 *
 * @param <A> The Avro type of the elements in the iterable.
 */
public class UnwrappedAvroValueIterable<A> implements Iterable<A> {
  /**
   * Implementation of Iterator over Avro messages that just unwraps
   * the message from the containing AvroValue.
   *
   * @param <E> The avro type of the elements in the iterable.
   */
  private static class UnwrappedAvroValueIterator<E> implements Iterator<E> {
    /** The wrapped AvroValue Iterator. */
    private Iterator<AvroValue<E>> mWrappedIterator;

    /**
     * Wrap an existing AvroValue Iterator.
     *
     * @param wrapped The AvroValue iterator to wrap.
     */
    public UnwrappedAvroValueIterator(Iterator<AvroValue<E>> wrapped) {
      mWrappedIterator = wrapped;
    }

    @Override
    public boolean hasNext() {
      return mWrappedIterator.hasNext();
    }

    @Override
    public E next() {
      return mWrappedIterator.next().datum();
    }

    @Override
    public void remove() {
      mWrappedIterator.remove();
    }
  }

  /** The wrapped AvroValue Iterable. */
  private Iterable<AvroValue<A>> mWrappedIterable;

  /**
   * Wrap an existing AvroValue Iterable.
   *
   * @param wrapped The AvroValue iterable object to wrap.
   */
  public UnwrappedAvroValueIterable(Iterable<AvroValue<A>> wrapped) {
    mWrappedIterable = wrapped;
  }

  @Override
  public Iterator<A> iterator() {
    return new UnwrappedAvroValueIterator<A>(mWrappedIterable.iterator());
  }
}
