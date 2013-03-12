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

package org.kiji.delegation;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNamedLookup {
  private static final Logger LOG = LoggerFactory.getLogger(TestNamedLookup.class);

  @Test
  public void testLookup() {
    NamedLookup<INamedFoo> lookup = Lookups.getNamed(INamedFoo.class);

    assertFalse(lookup.hasDuplicates());

    IFoo myFoo = lookup.lookup("B");
    LOG.info("Got foo implementation: " + myFoo.getClass().getName());
    assertTrue(myFoo instanceof Providers.NamedImplB);

    String result = myFoo.getMessage();
    LOG.info("Got result string: " + result);
    assertEquals("NamedB", result);

    IFoo foo2 = lookup.lookup("A");
    assertTrue(foo2 instanceof Providers.NamedImplA);
    result = foo2.getMessage();
    assertEquals("NamedA", result);
  }

  @Test
  public void testDups() {
    NamedLookup<IBar> lookup = Lookups.getNamed(IBar.class);

    assertTrue(lookup.hasDuplicates());

    IBar bar = lookup.lookup("B");
    String result = bar.getMessage();
    assertEquals("b", result);

    // Should get one of the two duplicate 'A' impls.
    bar = lookup.lookup("A");
    result = bar.getMessage();
    assertTrue("a1".equals(result) || "a2".equals(result));
  }
}

