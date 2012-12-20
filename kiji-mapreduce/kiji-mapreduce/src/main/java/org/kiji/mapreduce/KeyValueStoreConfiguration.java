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

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import org.kiji.mapreduce.context.NamespaceConfiguration;

/** Used to serialize KeyValueStore information into a unique namespace. */
public class KeyValueStoreConfiguration extends NamespaceConfiguration {
  /**
   * KeyValueStore definitions are serialized to the Configuration as a set of
   * keys under the "kiji.produce.kvstore.[i]" namespace.
   */
  // TODO(WIBI-1636): This is no longer a produce-specific variable,
  // so the namespacing should happen in "kiji.kvstore" instead.
  // Will this break existing jobs?  I don't think so, but I should think about it first.
  private static final String KEY_VALUE_STORE_NAMESPACE = "kiji.produce.kvstore.";

  /**
   * Factory method to copy a Configuration into a KeyValueStoreConfiguration.
   * The resulting KeyValueStoreConfiguration will have all key-value pairs
   * that the parent does.
   *
   * <p>Note that this does <i>not</i> wrap the Configuration <code>target</code>,
   * but instead copies values of it into a <i>new</i> Configuration.
   *
   * @param target The Configuration to copy into a KeyValueStoreConfiguration.
   * @return A new KeyValueStoreConfiguration, with each of the key-value pairs
   *     from <code>target</code>, but backed by a brand new Configuration.
   */
  public static KeyValueStoreConfiguration fromConf(Configuration target) {
    KeyValueStoreConfiguration theConf = new KeyValueStoreConfiguration(
        new Configuration(false), 0);
    for (Entry<String, String> e : target) {
      theConf.set(e.getKey(), e.getValue());
    }
    return theConf;
  }

  /**
   * Creates a KeyValueStoreConfiguratoin that writes to the <code>storeIndex</code>th
   * KeyValueStore namespace.
   *
   * @param parent The parent Configuration to back data.
   * @param storeIndex The namespace index to write to.
   */
  public KeyValueStoreConfiguration(Configuration parent, int storeIndex) {
    super(parent, KEY_VALUE_STORE_NAMESPACE + storeIndex);
  }
}
