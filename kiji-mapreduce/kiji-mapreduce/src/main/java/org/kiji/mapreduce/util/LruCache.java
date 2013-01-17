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

package org.kiji.mapreduce.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.kiji.annotations.ApiAudience;

/**
 * LRU cache based on LinkedHashMap.
 *
 * <p>This cache will retain 'maxSize' elements, based on the most recently-read
 * items. The get() operation is a structural modification to the underlying
 * data store.</p>
 *
 * @param <K> the key type in the map.
 * @param <V> the value type in the map.
 */
@SuppressWarnings("serial")
@ApiAudience.Private
public class LruCache<K, V> extends LinkedHashMap<K, V> {
  // Code based on http://littletechsecrets.wordpress.com/2008/11/16/simple-lru-cache-in-java/

  private static final long serialVersionUID = 1;

  private int mMaxSize;

  /**
   * Construct a cache that holds up to maxSize elements.
   *
   * @param maxSize the maximum number of elements the cache can hold.
   */
  public LruCache(int maxSize) {
    // Create an access-oriented cache that holds the user's max objects + 1 intermediate obj.
    super(maxSize + 1, 1, true);
    mMaxSize = maxSize;
  }

  /** {@inheritDoc} */
  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    // After size exceeds max entries, this statement returns true and the
    // oldest value will be removed. Since this map is access oriented the
    // oldest value would be least recently used.
    return size() > mMaxSize;
  }

  /** @return the maximum number of objects to cache. */
  public int getMaxSize() {
    return mMaxSize;
  }
}
