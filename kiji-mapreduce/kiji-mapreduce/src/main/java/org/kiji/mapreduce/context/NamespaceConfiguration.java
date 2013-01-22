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

package org.kiji.mapreduce.context;

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * <p>A Configuration backed by a namespace in a parent
 * {@link org.apache.hadoop.conf.Configuration}.</p>
 *
 * <p><code>setFoo("my-var", "my-val")</code> is equivalent to calling
 * <code>getDelegate().setFoo(getNamespace() + "my-var", "my-val")</code></p>.
 */
@ApiAudience.Private
@Inheritance.Sealed
public class NamespaceConfiguration {
  /** The parent Configuration to write to. */
  private final Configuration mDelegate;
  /** The namespace to write in. */
  private final String mNamespace;

  /**
   * Constructs a new NamespaceConfiguration that will read and write
   * values to the parent Configuration, under the specified namespace.
   *
   * @param parent The Configuration to back this NamespaceConfiguration.
   * @param namespace The namespace to write to.
   */
  public NamespaceConfiguration(Configuration parent, String namespace) {
    if (null == parent || null == namespace) {
      throw new IllegalArgumentException("Parent configuration and namespace must be non-null.");
    }
    mDelegate = parent;
    mNamespace = namespace;
  }

  /**
   * Returns the parent Configuration that backs this NamespaceConfiguration.
   *
   * @return The parent Configuration.
   */
  public Configuration getDelegate() {
    return mDelegate;
  }

  /**
   * Returns the namespace that this NamespaceConfiguration works in.
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
   * Returns the prefix for all keys stored in this NamespaceConfiguration.
   *
   * @param name The String to prefix with the namespace.
   * @return The prefix for all keys stored in this NamespaceConfiguration.
   */
  protected String prefix(String name) {
    return getNamespace() + "." + name;
  }
}
