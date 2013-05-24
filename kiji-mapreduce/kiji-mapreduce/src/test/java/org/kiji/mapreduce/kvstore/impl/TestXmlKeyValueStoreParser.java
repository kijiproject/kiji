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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.SeqFileKeyValueStore;
import org.kiji.schema.KijiClientTest;

public class TestXmlKeyValueStoreParser extends KijiClientTest {

  /**
   * A KeyValueStore that requires in its initFromConf() method that Hadoop
   * default values be included.
   */
  public static class DefaultsRequiredKVStore implements KeyValueStore<Object, Object> {
    @Override
    public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
      // Do nothing.
    }

    @Override
    public void initFromConf(KeyValueStoreConfiguration kvConf) throws IOException {
      // Fail if the provided Configuration hasn't loaded core-default.xml.
      // Check for both a Hadoop 1.x guaranteed-present key or a Hadoop 2.x key.
      Configuration conf = kvConf.getDelegate();
      if (conf.get("fs.AbstractFileSystem.hdfs.impl") == null && conf.get("fs.hdfs.impl") == null) {
        fail("Could not find HDFS implementation declared in conf; defaults don't seem loaded.");
      }
    }

    @Override
    public KeyValueStoreReader<Object, Object> open() throws IOException {
      return null;
    }
  }

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

  private Configuration mConf = new Configuration();

  @Test
  public void testSingleStore() throws IOException {
    // Test that a single store can be bound from this.
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
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
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
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
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
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
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
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
    Map<String, KeyValueStore<?, ?>> stores = XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
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

  @Test
  public void testAliasedStores() throws IOException {
    try {
      // Test that multiple stores with the same name cause an IOException.
      XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
          stringAsInputStream("<stores>\n"
            + "  <store class=\"" + SeqFileKeyValueStore.class.getCanonicalName() + "\""
            + " name=\"foo\">\n"
            + "    <configuration/>\n"
            + "  </store>\n"
            + "  <store class=\"EmptyKeyValueStore\" name=\"foo\">\n"
            + "    <configuration/>\n"
            + "  </store>\n"
            + "</stores>"));
      fail("Should have thrown an IOException.");
    } catch (IOException ioe) {
      assertEquals("Store with name \"foo\" is defined multiple times", ioe.getMessage());
    }
  }

  @Test
  public void testInvalidParse() throws IOException {
    try {
      // Test that invalid XML in the top level throws an exception.
      XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
          stringAsInputStream("<stores>\n"
            + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
            + "    <configuration>\n"
            + "    </configuration>\n"
            + "  </store>\n"
            + "  <someIllegalElement/>\n"
            + "</stores>"));

      fail("Should have thrown an IOException.");
    } catch (IOException ioe) {
      assertEquals("Unexpected first-level element: someIllegalElement", ioe.getMessage());
    }
  }

  @Test
  public void testInvalidParse2() throws IOException {
    try {
      // Test that invalid XML in a <store> element fails.
      XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
          stringAsInputStream("<stores>\n"
            + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
            + "    <blort/>\n"
            + "  </store>\n"
            + "  <someIllegalElement/>\n"
            + "</stores>"));

      fail("Should have thrown an IOException.");
    } catch (IOException ioe) {
      assertEquals("Unrecognized XML schema for store meep; expected <configuration> element.",
          ioe.getMessage());
    }
  }

  @Test
  public void testInvalidParse3() throws IOException {
    try {
      // Test that invalid XML in a <store> element fails.
      XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
          stringAsInputStream("<stores>\n"
            + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
            + "    <configuration/>\n"
            + "    <blort/>\n"
            + "  </store>\n"
            + "  <someIllegalElement/>\n"
            + "</stores>"));

      fail("Should have thrown an IOException");
    } catch (IOException ioe) {
      assertEquals("Unrecognized XML schema for store meep; expected <configuration> element.",
          ioe.getMessage());
    }
  }

  @Test
  public void testInvalidParseInConfiguration() throws IOException {
    try {
      // Test that invalid XML in the <configuration> element throws an IOException.
      XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
          stringAsInputStream("<stores>\n"
            + "  <store class=\"EmptyKeyValueStore\" name=\"meep\">\n"
            + "    <configuration>\n"
            + "      <someIllegalElement/>\n"
            + "    </configuration>\n"
            + "  </store>\n"
            + "</stores>"));

      fail("Should have thrown an IOException.");
    } catch (IOException ioe) {
      assertEquals("Unexpected element in configuration: someIllegalElement",
          ioe.getMessage());
    }
  }

  @Test
  public void testConfigDefaultsRequiredStores() throws IOException {
    // Test that the store in question is configured with a full Configuration instance.
    XmlKeyValueStoreParser.get(mConf).loadStoresFromXml(
        stringAsInputStream("<stores>\n"
        + "  <store class=\"" + DefaultsRequiredKVStore.class.getName() + "\" name=\"meep\">\n"
        + "    <configuration>\n"
        + "    </configuration>\n"
        + "  </store>\n"
        + "</stores>"));
  }
}
