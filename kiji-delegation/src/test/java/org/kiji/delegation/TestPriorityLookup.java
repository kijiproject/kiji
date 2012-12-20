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

package org.kiji.delegation;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPriorityLookup {
  private static final Logger LOG = LoggerFactory.getLogger(TestPriorityLookup.class);

  @Test
  public void testLookup() {
    Lookup<IPriorityFoo> lookup = Lookups.getPriority(IPriorityFoo.class);

    // This should only be ImplB, which has the highest priority.
    IFoo myFoo = lookup.lookup();
    LOG.info("Got foo implementation: " + myFoo.getClass().getName());
    assertTrue(myFoo instanceof Providers.PriorityImplB);

    String result = myFoo.getMessage();
    LOG.info("Got result string: " + result);
    assertEquals("priorityimplB", result);
  }

  @Test
  public void testMultiLookup() {
    Lookup<IPriorityFoo> lookup = Lookups.getPriority(IPriorityFoo.class);

    // We should get back a list of providers, ImplB followed by ImplA,
    // according to their priority order.
    List<IFoo> foos = new ArrayList<IFoo>();
    for (IFoo foo : lookup) {
      foos.add(foo);
    }

    assertEquals("expected two implementations", 2, foos.size());

    LOG.info("First impl class: " + foos.get(0).getClass().getName());
    LOG.info("Second impl class: " + foos.get(1).getClass().getName());

    assertTrue("expected implb first", foos.get(0) instanceof Providers.PriorityImplB);
    assertTrue("expected impla next", foos.get(1) instanceof Providers.PriorityImplA);
  }
}

