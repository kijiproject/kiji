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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Tests that the LruCache works. */
public class TestLruCache {

  @Test
  public void testCache() {
    LruCache<String, String> cache = new LruCache<String, String>(4);
    cache.put("a", "a1");
    cache.put("b", "b1");
    cache.put("c", "c1");
    cache.put("d", "d1");
    cache.put("e", "e1");
    assertFalse(cache.containsKey("a")); // e evicts a.

    assertEquals("b1", cache.get("b")); // This is still there.
    cache.put("f", "f1");
    assertFalse(cache.containsKey("c")); // f now evicts c, since b was recently used.

    cache.put("g", null);
    assertTrue(cache.containsKey("g"));
    assertNull(cache.get("g"));
    assertFalse(cache.containsKey("d")); // d evicted by g.
    assertNull(cache.get("d")); // But accessing this just returns null, not an error.
  }
}
