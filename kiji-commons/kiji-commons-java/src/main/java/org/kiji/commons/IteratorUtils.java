/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.commons;

import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Utilities for working with {@link Iterator}s. Also see {@link Iterators}.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class IteratorUtils {

  /**
   * Wraps an existing iterator and removes consecutive duplicate elements.
   *
   * @param iterator The iterator to deduplicate.
   * @param <T> The value type of the iterator.
   * @return An iterator which lazily deduplicates elements.
   */
  public static <T> Iterator<T> deduplicatingIterator(final Iterator<T> iterator) {
    return new DeduplicatingIterator<T>(iterator);
  }

  /**
   * A deduplicating iterator which removes consecutive duplicate elements from another iterator.
   *
   * @param <T> The value type of elements in the iterator.
   */
  @NotThreadSafe
  private static final class DeduplicatingIterator<T> extends UnmodifiableIterator<T> {
    private final PeekingIterator<T> mIterator;

    /**
     * Create an iterator which will remove consecutive duplicate elements in an iterator.
     *
     * @param iterator The iterator to deduplicate.
     */
    private DeduplicatingIterator(final Iterator<T> iterator) {
      mIterator = Iterators.peekingIterator(iterator);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mIterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
      final T next = mIterator.next();

      while (mIterator.hasNext() && next.equals(mIterator.peek())) {
        mIterator.next();
      }

      return next;
    }
  }

  /** Private constructor for utility class. */
  private IteratorUtils() { }
}
