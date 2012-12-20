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

package org.kiji.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import org.kiji.mapreduce.context.NamespaceConfiguration;

public class TestNamespaceConfiguration {
  @Test
  public void testStoreString() {
    Configuration parent = new Configuration(false);
    NamespaceConfiguration isolated = new NamespaceConfiguration(parent, "the.namespace");
    isolated.set("foo-key", "foo-value");
    assertEquals("foo-value", isolated.get("foo-key"));

    // Check that this value is stored in the namespace on the parent:
    assertEquals("foo-value", isolated.getDelegate().get("the.namespace.foo-key"));
  }

  @Test
  public void testStoreBoolean() {
    final boolean expected = true;
    Configuration parent = new Configuration(false);
    NamespaceConfiguration isolated = new NamespaceConfiguration(parent, "the.namespace");
    isolated.setBoolean("foo-key", expected);
    assertEquals(expected, isolated.getBoolean("foo-key", !expected));

    // Check that this value is stored in the namespace on the parent:
    assertEquals(expected, isolated.getDelegate().getBoolean("the.namespace.foo-key", !expected));
  }

  @Test
  public void testStoreClass() {
    Configuration parent = new Configuration(false);
    NamespaceConfiguration isolated = new NamespaceConfiguration(parent, "the.namespace");
    isolated.setClass("foo-key", String.class, Object.class);
    assertEquals(String.class, isolated.getClass("foo-key", null));
    assertEquals(String.class, isolated.getClass("foo-key", null, Object.class));

    // Check that this value is stored in the namespace on the parent:
    assertEquals(String.class, isolated.getDelegate().getClass("the.namespace.foo-key", null));
  }

  @Test
  public void testStoreFloat() {
    Configuration parent = new Configuration(false);
    NamespaceConfiguration isolated = new NamespaceConfiguration(parent, "the.namespace");
    isolated.setFloat("foo-key", 3.14F);
    assertEquals(3.14F, isolated.getFloat("foo-key", 0.0F), 0.0F);

    // Check that this value is stored in the namespace on the parent:
    assertEquals(3.14F, isolated.getDelegate().getFloat("the.namespace.foo-key", 0.0F), 0.0F);
  }

  @Test
  public void testStoreInt() {
    Configuration parent = new Configuration(false);
    NamespaceConfiguration isolated = new NamespaceConfiguration(parent, "the.namespace");
    isolated.setInt("foo-key", 123);
    assertEquals(123, isolated.getInt("foo-key", 0));

    // Check that this value is stored in the namespace on the parent:
    assertEquals(123, isolated.getDelegate().getInt("the.namespace.foo-key", 0));
  }

  @Test
  public void testStoreLong() {
    Configuration parent = new Configuration(false);
    NamespaceConfiguration isolated = new NamespaceConfiguration(parent, "the.namespace");
    isolated.setLong("foo-key", 12345L);
    assertEquals(12345L, isolated.getLong("foo-key", 0L));

    // Check that this value is stored in the namespace on the parent:
    assertEquals(12345L, isolated.getDelegate().getLong("the.namespace.foo-key", 0L));
  }

  @Test
  public void testStoreStringArray() {
    Configuration parent = new Configuration(false);
    NamespaceConfiguration isolated = new NamespaceConfiguration(parent, "the.namespace");
    isolated.setStrings("foo-key", "first", "second", "third");
    assertTrue(Arrays.equals(new String[] {"first", "second", "third"},
        isolated.getStrings("foo-key")));
    // Also test the specifying a defaultValue works.
    assertTrue(Arrays.equals(new String[] {"first", "second", "third"},
        isolated.getStrings("foo-key", new String[0])));

    // Check that this value is stored in the namespace on the parent:
    assertTrue(Arrays.equals(new String[] {"first", "second", "third"},
        isolated.getDelegate().getStrings("the.namespace.foo-key")));
  }
}
