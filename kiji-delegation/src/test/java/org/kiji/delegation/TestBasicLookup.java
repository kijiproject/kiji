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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBasicLookup {
  private static final Logger LOG = LoggerFactory.getLogger(TestBasicLookup.class);

  @Test
  public void testLookup() {
    Lookup<IFoo> lookup = Lookups.get(IFoo.class);
    IFoo myFoo = lookup.lookup();
    LOG.info("Got foo implementation: " + myFoo.getClass().getName());
    String result = myFoo.getMessage();
    assertNotNull(result);
    LOG.info("Got result string: " + result);
    assertTrue("Got a weird string back from IFoo", result.startsWith("impl"));
  }
}

