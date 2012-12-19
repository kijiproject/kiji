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

package org.kiji.mapreduce.kvstore;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import org.kiji.mapreduce.KeyValueStore;
import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.schema.KijiColumnName;

public class TestKijiTableKeyValueStore {
  @Test
  public void testSerialization() throws IOException {
    // Test that we can serialize a KijiTableKeyValueStore to a conf and resurrect it.
    KijiTableKeyValueStore<String> input = new KijiTableKeyValueStore<String>();

    input.setTableName("tbl");
    input.setColumn(new KijiColumnName("some:column"));
    input.setMinTimestamp(42);
    input.setMaxTimestamp(512);
    input.setCacheLimit(2121);
    input.setReaderSchema(Schema.create(Schema.Type.STRING));

    KeyValueStoreConfiguration conf = KeyValueStoreConfiguration.fromConf(new Configuration());

    input.storeToConf(conf);

    // TODO(WIBI-1534): Update the variable name that maps to the store name.
    conf.getDelegate().set("kiji.produce.kvstore.name", "the-store-name");

    KijiTableKeyValueStore<String> output = new KijiTableKeyValueStore<String>();
    output.initFromConf(conf);

    assertEquals(input, output);
  }

  @Test
  public void testOkWithoutSchema() throws IOException {
    // Serializing without an explicit reader schema is ok.
    KijiTableKeyValueStore<String> input = new KijiTableKeyValueStore<String>();

    input.setTableName("tbl");
    input.setColumn(new KijiColumnName("some:column"));
    input.setMinTimestamp(42);
    input.setMaxTimestamp(512);
    input.setCacheLimit(2121);

    KeyValueStoreConfiguration conf = KeyValueStoreConfiguration.fromConf(
        new Configuration(false));

    input.storeToConf(conf);
    conf.set(KeyValueStore.CONF_NAME, "the-store-name");

    KijiTableKeyValueStore<String> output = new KijiTableKeyValueStore<String>();
    output.initFromConf(conf);

    assertEquals(input, output);
  }

  @Test(expected=IOException.class)
  public void testRequiresTable() throws IOException {
    // Test that we need to set the table name, or it will fail to verify as input.
    KijiTableKeyValueStore<String> input = new KijiTableKeyValueStore<String>();

    input.setColumn(new KijiColumnName("some:column"));

    KeyValueStoreConfiguration conf = KeyValueStoreConfiguration.fromConf(
        new Configuration(false));
    input.storeToConf(conf);
  }

  @Test(expected=IOException.class)
  public void testRequiresColumn() throws IOException {
    // Test that we need to set the column to read.
    KijiTableKeyValueStore<String> input = new KijiTableKeyValueStore<String>();

    input.setTableName("foo");

    KeyValueStoreConfiguration conf = KeyValueStoreConfiguration.fromConf(
        new Configuration(false));
    input.storeToConf(conf);
  }
}
