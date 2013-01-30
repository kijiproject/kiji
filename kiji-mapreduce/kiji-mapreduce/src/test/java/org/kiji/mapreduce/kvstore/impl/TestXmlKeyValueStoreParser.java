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

package org.kiji.mapreduce.kvstore.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.SeqFileKeyValueStore;

public class TestXmlKeyValueStoreParser {
  /**
   * @param data a string to wrap in an input stream.
   * @return the input 'data' wrapped in an InputStream object.
   */
  private InputStream stringAsInputStream(String data) {
    try {
      return new ByteArrayInputStream(data.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      fail("Your jvm is broken because it doesn't support UTF-8: " + uee.getMessage());
      return null; // Never hit.
    }
  }

  @Test
  public void testSingleStore() throws IOException {
    // Test that a single store can be bound from this.
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"" + EmptyKeyValueStore.class.getCanonicalName() + "\""
        + " name=\"meep\">\n"
        + "    <configuration/>\n"
        + "  </store>\n"
        + "</stores>"));
    assertNotNull(stores);
    assertEquals(1, stores.size());
    assertNotNull(stores.get("meep"));
    assertEquals(EmptyKeyValueStore.class, stores.get("meep").getClass());
  }

  @Test
  public void testDefaultPackagePrefix() throws IOException {
    // Test that the default package prefix will be attached to a class name.
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
        + "    <configuration/>\n"
        + "  </store>\n"
        + "</stores>"));
    assertNotNull(stores);
    assertEquals(1, stores.size());
    assertNotNull(stores.get("meep"));
    assertEquals(EmptyKeyValueStore.class, stores.get("meep").getClass());
  }

  @Test
  public void testZeroConfigStore() throws IOException {
    // Test that a store with no inner <configuration> is ok.
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"" + EmptyKeyValueStore.class.getCanonicalName() + "\""
        + " name=\"meep\">\n"
        + "  </store>\n"
        + "</stores>"));
    assertNotNull(stores);
    assertEquals(1, stores.size());
    assertNotNull(stores.get("meep"));
    assertEquals(EmptyKeyValueStore.class, stores.get("meep").getClass());
  }

  @Test
  public void testStoreWithParameters() throws IOException {
    // Test that a store with actual parameters can be bound by this mechanism
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"" + SeqFileKeyValueStore.class.getCanonicalName() + "\""
        + " name=\"meep\">\n"
        + "    <configuration>\n"
        + "      <property>\n"
        + "        <name>paths</name>\n"
        + "        <value>/user/aaron/foo.seq</value>\n"
        + "      </property>\n"
        + "    </configuration>\n"
        + "  </store>\n"
        + "</stores>"));
    assertNotNull(stores);
    assertEquals(1, stores.size());
    assertNotNull(stores.get("meep"));
    assertEquals(SeqFileKeyValueStore.class, stores.get("meep").getClass());
    SeqFileKeyValueStore<?, ?> seqStore = (SeqFileKeyValueStore<?, ?>) stores.get("meep");
    List<Path> inputPaths = seqStore.getInputPaths();
    assertNotNull(inputPaths);
    assertEquals(1, inputPaths.size());
    assertEquals(new Path("/user/aaron/foo.seq"), inputPaths.get(0));
  }

  @Test
  public void testMultipleStores() throws IOException {
    // Test that this can process multiple stores.
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"" + SeqFileKeyValueStore.class.getCanonicalName() + "\""
        + " name=\"foo\">\n"
        + "    <configuration>\n"
        + "      <property>\n"
        + "        <name>paths</name>\n"
        + "        <value>/user/aaron/foo.seq</value>\n"
        + "      </property>\n"
        + "    </configuration>\n"
        + "  </store>\n"
        + "  <store class=\"EmptyKeyValueStore\" name=\"bar\">\n"
        + "    <configuration/>\n"
        + "  </store>\n"
        + "</stores>"));

    assertNotNull(stores);
    assertEquals(2, stores.size());
    assertNotNull(stores.get("foo"));
    assertNotNull(stores.get("bar"));
    assertEquals(SeqFileKeyValueStore.class, stores.get("foo").getClass());
    assertEquals(EmptyKeyValueStore.class, stores.get("bar").getClass());
    SeqFileKeyValueStore<?, ?> seqStore = (SeqFileKeyValueStore<?, ?>) stores.get("foo");
    List<Path> inputPaths = seqStore.getInputPaths();
    assertNotNull(inputPaths);
    assertEquals(1, inputPaths.size());
    assertEquals(new Path("/user/aaron/foo.seq"), inputPaths.get(0));
  }

  @Test(expected=IOException.class)
  public void testAliasedStores() throws IOException {
    // Test that multiple stores with the same name cause an IOException.
    XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"" + SeqFileKeyValueStore.class.getCanonicalName() + "\""
        + " name=\"foo\">\n"
        + "    <configuration/>\n"
        + "  </store>\n"
        + "  <store class=\"EmptyKeyValueStore\" name=\"foo\">\n"
        + "    <configuration/>\n"
        + "  </store>\n"
        + "</stores>"));

    fail("We didn't expect to get this far.");
  }

  @Test(expected=IOException.class)
  public void testInvalidParse() throws IOException {
    // Test that invalid XML in the top level throws an exception.
    XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
        + "    <configuration>\n"
        + "    </configuration>\n"
        + "  </store>\n"
        + "  <someIllegalElement/>\n"
        + "</stores>"));

    fail("Didn't expect to get this far.");
  }

  @Test(expected=IOException.class)
  public void testInvalidParse2() throws IOException {
    // Test that invalid XML in a <store> element fails.
    XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
        + "    <blort/>\n"
        + "  </store>\n"
        + "  <someIllegalElement/>\n"
        + "</stores>"));

    fail("Didn't expect to get this far.");
  }

  @Test(expected=IOException.class)
  public void testInvalidParse3() throws IOException {
    // Test that invalid XML in a <store> element fails.
    XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
        + "    <configuration/>\n"
        + "    <blort/>\n"
        + "  </store>\n"
        + "  <someIllegalElement/>\n"
        + "</stores>"));

    fail("Didn't expect to get this far.");
  }

  @Test(expected=IOException.class)
  public void testInvalidParseInConfiguration() throws IOException {
    // Test that invalid XML in the <configuration> element throws an IOException.
    XmlKeyValueStoreParser.get().loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
        + "    <configuration>\n"
        + "      <someIllegalElement/>\n"
        + "    </configuration>\n"
        + "  </store>\n"
        + "</stores>"));

    fail("Didn't expect to get this far.");
  }
}
