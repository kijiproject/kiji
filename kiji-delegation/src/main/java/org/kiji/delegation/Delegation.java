/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.lang.reflect.Field;

import org.kiji.annotations.ApiAudience;

/**
 * Static methods used to configure objects through the kiji-delegation library.
 *
 * The most commonly-used method is {@link Delegation#init(Object)}, which intializes
 * fields of classes and objects decorated with the {@link AutoLookup} annotation.
 * This annotation indicates that the specified field of a class or object should be
 * auto-initialized through a {@link ConfiguredLookup}.
 *
 * <p>This allows you to use runtime-configured dependency injection to specify
 * the implementation, like so:</p>
 *
 * <div><tt><pre>class Foo {
 *   @AutoLookup
 *   private Bar myBar;
 *
 *   public Foo() {
 *     Delegation.init(this); // Populate AutoLookup fields.
 *     // Use myBar here.
 *   }
 * }</pre></tt></div>
 *
 * <p>The implementation of the <tt>Bar</tt> interface or abstract class will be chosen
 * by consulting the kiji-delegation.properties resource(s) on the classpath. See
 * {@link ConfiguredLookup} for more information.</p>
 */
@ApiAudience.Public
public final class Delegation {

  /** Private constructor; this should not be instantiated. */
  private Delegation() { }

  /**
   * Initialize all {@link AutoLookup} fields of obj, using the current thread's
   * context ClassLoader.
   *
   * <p>Fields with non-null values are ignored, to prevent multiple calls to {@link #init}
   * from accidentally overwriting values already in use. You must explicitly null out fields
   * before reinitializing them.</p>
   *
   * @param obj the object to initialize. Must not be null.
   * @throws NoSuchProviderException if a field cannot be initialized with a default implementation.
   */
  public static void init(Object obj) {
    init(obj, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Initialize all {@link AutoLookup} fields of obj.
   *
   * <p>Fields with non-null values are ignored, to prevent multiple calls to {@link #init}
   * from accidentally overwriting values already in use. You must explicitly null out fields
   * before reinitializing them.</p>
   *
   * @param obj the object to initialize. Must not be null.
   * @param classLoader the classLoader to use.
   * @throws NoSuchProviderException if a field cannot be initialized with a default implementation.
   */
  public static void init(Object obj, ClassLoader classLoader) {
    for (Field field : obj.getClass().getDeclaredFields()) {
      boolean accessibility = field.isAccessible();
      field.setAccessible(true);
      try {
        if (field.getAnnotation(AutoLookup.class) != null && field.get(obj) == null) {
          // This field is marked @AutoLookup and its value is null, initialize it.
          Class<?> fieldClass = field.getType();
          Lookup<?> fieldLookup = Lookups.getConfigured(fieldClass, classLoader);
          Object value = fieldLookup.lookup(); // Get a new instance of the underlying type.

          field.set(obj, value); // Set the value of the field on this object.
        }
      } catch (IllegalAccessException iae) {
        throw new RuntimeException("Could not initialize field " + field.getName(), iae);
      } finally {
        field.setAccessible(accessibility); // restore this property of the field.
      }
    }
  }
}
