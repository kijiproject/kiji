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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class TestIteratorUtils {

  @Test
  public void testDeduplicatingIterator() {

    final List<String> duplicates =
        ImmutableList.of("foo", "bar", "bar", "baz", "baz", "baz", "foo");
    final List<String> deduplicates = ImmutableList.of("foo", "bar", "baz", "foo");

    Assert.assertEquals(
        deduplicates,
        ImmutableList.copyOf(IteratorUtils.deduplicatingIterator(duplicates.iterator())));
  }
}
