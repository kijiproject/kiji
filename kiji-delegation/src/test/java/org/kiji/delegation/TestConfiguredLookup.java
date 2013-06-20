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

public class TestConfiguredLookup {
  private static final Logger LOG = LoggerFactory.getLogger(TestConfiguredLookup.class);

  @Test
  public void testFullyConfiguredLookup() {
    // When an implementation class is specified in the config file, we should use it.
    Lookup<ConfiguredIfaceA> lookup = Lookups.getConfigured(ConfiguredIfaceA.class);
    ConfiguredIfaceA myImpl = lookup.lookup();
    assertEquals("Hello", myImpl.getMessage());
  }

  @Test
  public void testMultipleInstances() {
    // ConfiguredLookup should return a new instance every time.
    Lookup<ConfiguredIfaceA> lookup = Lookups.getConfigured(ConfiguredIfaceA.class);
    ConfiguredIfaceA impl1 = lookup.lookup();
    ConfiguredIfaceA impl2 = lookup.lookup();

    assertTrue("Got the same object back and we shouldn't have", impl1 != impl2);
  }

  @Test
  public void testPreferConfToBasicLookup() {
    // When an implementation is bound in both the config file and the BasicLookup,
    // use the version from the config file.

    Lookup<ConfiguredIfaceB> lookup = Lookups.getConfigured(ConfiguredIfaceB.class);
    ConfiguredIfaceB myImpl = lookup.lookup();
    assertEquals(1, myImpl.getVal()); // ImplB1 returns 1.
  }

  @Test
  public void testFallBackToBasicLookup() {
    // When an implementation is not bound in the config file, use what's in the BasicLookup.

    Lookup<ConfiguredIfaceC> lookup = Lookups.getConfigured(ConfiguredIfaceC.class);
    ConfiguredIfaceC myImpl = lookup.lookup();
    assertEquals("SomeString", myImpl.getSomeString());
  }

  @Test
  public void testBasicLookupButStillMultiInstances() {
    // Even if we fall back to a BasicLookup, we should get a different instance every time.
    Lookup<ConfiguredIfaceC> lookup = Lookups.getConfigured(ConfiguredIfaceC.class);
    ConfiguredIfaceC impl1 = lookup.lookup();
    ConfiguredIfaceC impl2 = lookup.lookup();
    assertTrue("Got the same object back and we shouldn't have", impl1 != impl2);
  }

}

