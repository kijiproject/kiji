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

package org.kiji.mapreduce.kvstore.framework;

import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;

/**
 * Used to serialize KeyValueStore information into a unique namespace
 * inside a Configuration object. All conf keys written to this object (e.g., "foo")
 * are prefixed by a namespace ("ns") specific to the particular key-value store
 * instance (e.g., "ns.foo").
 */
@ApiAudience.Framework
public final class KeyValueStoreConfiguration {

  /** The parent Configuration to write to. */
  private final Configuration mDelegate;
  /** The namespace to write in. */
  private final String mNamespace;

  /**
   * KeyValueStore definitions are serialized to the Configuration as a set of
   * keys under the "kiji.kvstores.[i]" namespace.
   */
  public static final String KEY_VALUE_STORE_NAMESPACE = "kiji.job.kvstores.";

  /**
   * Factory method to wrap a Configuration in a KeyValueStoreConfiguration.
   *
   * <p>The resulting KeyValueStoreConfiguration will have all key-value pairs
   * that the Configuration does, stored as part of the "0"'th KeyValueStore
   * entry in the "kiji.job.kvstores" namespace in a new Configuration object
   * (e.g.,  calling <code>set("foo", ...)</code> on the returned object will
   * set key <tt>kiji.job.kvstores.0.foo</tt>).
   * </p>
   *
   * <p>Since this KeyValueStoreConfiguration will not be part of an existing
   * Configuration, this method is mostly useful for writing tests or creating
   * temporary key-value stores to merge into a Configuration later.</p>
   *
   * @param conf the Configuration to wrap in a KeyValueStoreConfiguration.
   * @return A new KeyValueStoreConfiguration containing the keys of the conf
   *     conf parameter. It will be backed by a new Configuration without loaded
   *     defaults.
   */
  public static KeyValueStoreConfiguration fromConf(Configuration conf) {
    KeyValueStoreConfiguration theConf = new KeyValueStoreConfiguration(
        new Configuration(false), 0);
    for (Entry<String, String> e : conf) {
      theConf.set(e.getKey(), e.getValue());
    }
    return theConf;
  }

  /**
   * Factory method to create a KeyValueStoreConfiguration that is backed by a
   * supplied Configuration.
   *
   * <p>The KeyValueStoreConfiguration will be created in the KeyValueStore namespace
   * at the supplied storeIndex. For example if <code>storeIndex</code> is 3, then
   * calling calling <code>set("foo", ...)</code> on the returned object will
   * set key <tt>kiji.job.kvstores.3.foo</tt> in <code>conf</code>.</p>
   *
   * <p>If the Configuration contains keys set in the <code>storeIndex</code>th
   * namespace, they will be reflected in the returned KeyValueStoreConfiguration.</p>
   *
   * @param conf The Configuration that will back the KeyValueStoreConfiguration.
   * @param storeIndex The namespace index to write to. Must be non-negative.
   * @return A KeyValueStoreConfiguration backed by <code>conf</code>.
   */
  public static KeyValueStoreConfiguration createInConfiguration(
      Configuration conf,
      int storeIndex) {
    Preconditions.checkArgument(storeIndex >= 0, "storeIndex must be non-negative.");
    return new KeyValueStoreConfiguration(conf, storeIndex);
  }

  /**
   * Helper function to generate the Configuration key that would be used for a key
   * in a KeyValueStoreConfiguration stored at a particular index. Useful for testing.
   *
   * @param key The key as it would be passed to a hypothetical
   *     KeyValueStoreConfiguration.
   * @param storeIndex The index of the hypothetical KeyValueStoreConfiguration.
   * @return The key that would be used for a backing Configuration.
   */
  public static String confKeyAtIndex(String key, int storeIndex) {
    return KEY_VALUE_STORE_NAMESPACE + storeIndex + "." + key;
  }

  /**
   * Creates a KeyValueStoreConfiguration that writes to the <code>storeIndex</code>th
   * KeyValueStore namespace.
   *
   * @param parent The parent Configuration to back data.
   * @param storeIndex The namespace index to write to.
   */
  private KeyValueStoreConfiguration(Configuration parent, int storeIndex) {
    this(parent, KEY_VALUE_STORE_NAMESPACE + storeIndex);
  }

  /**
   * Constructs a new KeyValueStoreConfiguration that will read and write
   * values to the parent Configuration, under an arbitrary namespace.
   *
   * @param parent The Configuration to back this KeyValueStoreConfiguration.
   * @param namespace The namespace to write to.
   */
  private KeyValueStoreConfiguration(Configuration parent, String namespace) {
    if (null == parent || null == namespace) {
      throw new IllegalArgumentException("Parent configuration and namespace must be non-null.");
    }
    mDelegate = parent;
    mNamespace = namespace;
  }

  /**
   * Returns the parent Configuration that backs this KeyValueStoreConfiguration.
   *
   * @return The parent Configuration.
   */
  public Configuration getDelegate() {
    return mDelegate;
  }

  /**
   * Returns the namespace that this KeyValueStoreConfiguration works in.
   *
   * @return The namespace.
   */
  public String getNamespace() {
    return mNamespace;
  }

  /**
   * Get the value of the <code>name</code> property, or null
   * if no such property exists.
   *
   * @param name The property to get a value for.
   * @return The value, or null, if no such property exists.
   */
  public String get(String name) {
    return mDelegate.get(prefix(name));
  }

  /**
   * Get the value of the <code>name</code> property.
   *
   * @param name The property to get a value for.
   * @param defaultValue The default value if none is specified.
   * @return The value.
   */
  public String get(String name, String defaultValue) {
    return mDelegate.get(prefix(name), defaultValue);
  }

  /**
   * Sets the <code>value</code> of the <code>name</code> property.
   *
   * @param name The property to set.
   * @param value The value to set.
   */
  public void set(String name, String value) {
    mDelegate.set(prefix(name), value);
  }

  /**
   * Get the value of the <code>name</code> property as a boolean.
   *
   * @param name The property to get a boolean for.
   * @param defaultValue The value to return if it is not set.
   * @return The value of this property as a boolean.
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    return mDelegate.getBoolean(prefix(name), defaultValue);
  }

  /**
   * Sets the value of the <code>name</code> property to a boolean.
   *
   * @param name The property to set.
   * @param value The boolean to set.
   */
  public void setBoolean(String name, boolean value) {
    mDelegate.setBoolean(prefix(name), value);
  }

  /**
   * Gets the value of the <code>name</code> property as a Class.
   *
   * @param name The property to get a Class for.
   * @param defaultValue The value to return if it is not set.
   * @return The value of this property as a Class.
   */
  public Class<?> getClass(String name, Class<?> defaultValue) {
    return mDelegate.getClass(prefix(name), defaultValue);
  }

  /**
   * Get the value of the <code>name</code> property as a Class implementing the
   * interface specified by <code>xface</code>.  If no such property is specified,
   * then <code>defaultValue</code> is returned.  An exception is thrown if the
   * returned class does not implement the named interface.
   *
   * @param <U> The interface to deserialize this class to.
   * @param name The property to get a Class for.
   * @param defaultValue The value to return if it is not set.
   * @param xface The interface implemented by the named class.
   * @return The value of this property as a Class implementing <code>xface</code>.
   */
  public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue,
      Class<U> xface) {
    return mDelegate.getClass(prefix(name), defaultValue, xface);
  }

  /**
   * Gets the value of the <code>name</code> property as a Class.
   *
   * @param name The property to get a Class for.
   * @return The value of this property as a Class.
   * @throws ClassNotFoundException If the class is not found.
   */
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    return mDelegate.getClassByName(prefix(name));
  }

  /**
   * Sets the value of the <code>name</code> property to be a Class.
   *
   * @param name The property to set.
   * @param theClass The Class to set.
   * @param xface The interface implemented by the named class.
   */
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    mDelegate.setClass(prefix(name), theClass, xface);
  }

  /**
   * Gets the value of the <code>name</code> property as a float.
   *
   * @param name The property to get a float for.
   * @param defaultValue The value to return if it is not set.
   * @return The value of this property as a float.
   */
  public float getFloat(String name, float defaultValue) {
    return mDelegate.getFloat(prefix(name), defaultValue);
  }

  /**
   * Sets the value of the <code>name</code> property to be a float.
   *
   * @param name The property to set.
   * @param value The float to set.
   */
  public void setFloat(String name, float value) {
    mDelegate.setFloat(prefix(name), value);
  }

  /**
   * Gets the value of the <code>name</code> property as an int.
   *
   * @param name The property to get an int for.
   * @param defaultValue The value to return if it is not set.
   * @return The value of this property as an int.
   */
  public int getInt(String name, int defaultValue) {
    return mDelegate.getInt(prefix(name), defaultValue);
  }

  /**
   * Sets the value of the <code>name</code> property to be an int.
   *
   * @param name The property to set.
   * @param value The int to set.
   */
  public void setInt(String name, int value) {
    mDelegate.setInt(prefix(name), value);
  }

  /**
   * Gets the value of the <code>name</code> property as a long.
   *
   * @param name The property to get a long for.
   * @param defaultValue The value to return if it is not set.
   * @return The value of this property as a long.
   */
  public long getLong(String name, long defaultValue) {
    return mDelegate.getLong(prefix(name), defaultValue);
  }

  /**
   * Sets the value of the <code>name</code> property to be a long.
   *
   * @param name The property to set.
   * @param value The long to set.
   */
  public void setLong(String name, long value) {
    mDelegate.setLong(prefix(name), value);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property
   * as an array of <code>String</code>s.
   *
   * @param name The property to get a String array for.
   * @return The value of this property as a String array.
   */
  public String[] getStrings(String name) {
    return mDelegate.getStrings(prefix(name));
  }

  /**
   * Get the comma delimited values of the <code>name</code> property
   * as an array of <code>String</code>s.  If none found, returns
   * the default value instead.
   *
   * @param name The property to get a String array for.
   * @param defaultValue The value to return if it is not set.
   * @return The value of this property as a String array.
   */
  public String[] getStrings(String name, String... defaultValue) {
    return mDelegate.getStrings(prefix(name), defaultValue);
  }

  /**
   * Set the array of string values for the <code>name</code> property as comma
   * delimited values.
   *
   * @param name The property to set.
   * @param values The values.
   */
  public void setStrings(String name, String... values) {
    mDelegate.setStrings(prefix(name), values);
  }

  /**
   * Get the value of the <code>name</code> property, without doing variable expansion.
   *
   * @param name The property to get a raw value for.
   * @return The raw value.
   */
  public String getRaw(String name) {
    return mDelegate.getRaw(prefix(name));
  }

  /**
   * Returns the specified name, prepended with the namespace prefix for all keys
   * stored in this KeyValueStoreConfiguration.
   *
   * @param name The String to prefix with the namespace.
   * @return The true key to write into the underlying Configuration; the namespace
   *     concatenated with a "." character and the specified name.
   */
  private String prefix(String name) {
    return getNamespace() + "." + name;
  }
}
