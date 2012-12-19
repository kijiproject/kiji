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

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestKeyValueStoreConfiguration {
  @Test
  public void testCopyFromConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set("foo", "foo-value");
    conf.setInt("bar", 123);
    conf.setClass("qaz", String.class, Object.class);

    KeyValueStoreConfiguration kvStoreConf = KeyValueStoreConfiguration.fromConf(conf);
    assertEquals("foo-value", kvStoreConf.get("foo"));
    assertEquals(123, kvStoreConf.getInt("bar", 0));
    assertEquals(String.class, kvStoreConf.getClass("qaz", null));
  }
}
