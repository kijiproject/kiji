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

package org.kiji.scoring.params;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.kiji.delegation.Lookup;
import org.kiji.delegation.Lookups;

/**
 * Everything there is to know about a param definition.
 */
public class ParamSpec {
  /** The Java field that should be assigned to with the param's value. */
  private final Field mField;

  /** The Param annotation. */
  private final Param mParam;

  /** The object that contains the param definition. */
  private final Object mObj;

  /** Parser for this param. */
  private final ValueParser<?> mParser;

  /**
   * Map from class type to a name suitable for display.
   */
  private static final Map<Class, String> CLASS_TO_NAME_MAP =
      ImmutableMap.<Class, String>builder()
      .put(String.class, "String")
      .put(Short.class, "Short")
      .put(List.class, "List<String>")
      .put(Set.class, "Set<String>")
      .put(Boolean.class, "Boolean")
      .put(Double.class, "Double")
      .put(Integer.class, "Integer")
      .put(Map.class, "Map<String,String>")
      .put(Float.class, "Float")
      .put(Long.class, "Long")
      .build();

  /**
   * Creates a new <code>ParamSpec</code> instance.
   *
   * @param field The Java field.
   * @param param The annotation on the field.
   * @param obj The object that contains the field.
   */
  public ParamSpec(Field field, Param param, Object obj) {
    mField = field;
    mParam = param;
    mObj = obj;
    mParser = findParser();
  }

  /**
   * Identifies the parser for this param.
   *
   * Requires mField and mParam being set properly.
   *
   * @return the argument parser for this param.
   */
  private ValueParser<?> findParser() {
    final Class<?> fieldClass = mField.getType();

    final ValueParser<?> simpleParser = PARSERS.get(fieldClass);
    if (simpleParser != null) {
      return simpleParser;
    }

    for (Map.Entry<Class<?>, ValueParser<?>> entry : BASE_PARSERS.entrySet()) {
      final Class<?> baseClass = entry.getKey();
      final ValueParser<?> baseClassParser = entry.getValue();
      if (baseClass.isAssignableFrom(fieldClass)) {
        return baseClassParser;
      }
    }

    // Failed to find a matching param parser:
    throw new UnsupportedParamTypeException(this);
  }

  /**
   * Gets the name of the param.
   *
   * @return The name of the param.
   */
  public String getName() {
    return mParam.name().isEmpty() ? mField.getName() : mParam.name();
  }

  /**
   * Gets the type of the field.
   *
   * @return The type of the field.
   */
  public Class<?> getType() {
    return mField.getType();
  }

  /**
   * Gets the type of the field a string.
   *
   * @return The type of the field.
   */
  public String getTypeName() {
    Class<?> type = getType();
    // Most built-in classes will have a value here. The exceptions are:
    // * Primitive types - their type.toString() is concise (e.g. "int"), so just use that.
    // * Enums - we get the full name. Not much we can do about it in general.
    String name = CLASS_TO_NAME_MAP.get(type);
    return (name != null) ? name : type.toString();
  }

  /**
   * Get the human-readable description for the param.
   *
   * @return The human-readable description.
   */
  public String getDescription() {
    return mParam.description();
  }

  /**
   * Determines whether the param is required.
   *
   * @return Whether the param is required.
   */
  public boolean isRequired() {
    return mParam.required();
  }

  /**
   * Determines whether the param should be shown when {@code --help} is provided.
   *
   * @return Whether the param is hidden.
   */
  public boolean isHidden() {
    return mParam.hidden();
  }

  /**
   * Gets the default value for the param as a string.
   *
   * @return The default value as a string.
   */
  public String getDefaultValue() {
    try {
      if (!mField.isAccessible()) {
        setFieldAccessible();
      }
      return String.format("%s", mField.get(mObj));
    } catch (IllegalAccessException e) {
      throw new IllegalAccessError(e.getMessage());
    }
  }

  /**
   * Assigns the value of the param to its field.
   *
   * @param value The value specified for this param.
   * @throws IllegalAccessException If the field cannot be assigned.
   */
  public void setValue(String value) throws IllegalAccessException {
    if (!mField.isAccessible()) {
      setFieldAccessible();
    }
    if (value == null) {
      if (mParam.required()) {
        throw new IllegalParamValueException(String.format(
            "Parameter '%s' is required but has not been specified.", getName()));
      }
    } else {
      mField.set(mObj, mParser.parse(this, value));
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return mField.getDeclaringClass().getName() + "." + mField.getName();
  }

  /**
   * Set the field to be accessible, so that it can be modified by reflection.
   */
  private void setFieldAccessible() {
    AccessController.doPrivileged(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        mField.setAccessible(true);
        return null;
      }
    });
  }

  /**
   * Map from param value class to param value parser.
   *
   * The parsers in this map parse exactly the class specified in the key.
   */
  private static final ImmutableMap<Class<?>, ValueParser<?>> PARSERS;

  /**
   * Map from param value base class to param value parser.
   *
   * The parsers in this map parse all subclasses of the class specified in the key.
   */
  private static final ImmutableMap<Class<?>, ValueParser<?>> BASE_PARSERS;

  /** Initializes the parsers maps. */
  static {
    final Map<Class<?>, ValueParser<?>> parserMap = Maps.newHashMap();
    final Map<Class<?>, ValueParser<?>> baseParserMap = Maps.newHashMap();
    final Lookup<ValueParser> lookup = Lookups.get(ValueParser.class);
    buildParserMap(lookup, parserMap, baseParserMap);
    PARSERS = ImmutableMap.copyOf(parserMap);
    BASE_PARSERS = ImmutableMap.copyOf(baseParserMap);
  }

  /**
   * Builds the argument parser maps.
   *
   * @param parsers Iterable of ValueParser to register.
   * @param parserMap Map of parsers for exact value types to fill in.
   * @param baseParserMap Map of parsers for subclass types to fill in.
   * @throws RuntimeException if there are conflicts on the value types to register parsers for.
   */
  public static void buildParserMap(
      Iterable<ValueParser> parsers,
      Map<Class<?>, ValueParser<?>> parserMap,
      Map<Class<?>, ValueParser<?>> baseParserMap) {
    for (ValueParser<?> parser : parsers) {
      final Class<?> klass = parser.getParsedClass();
      if (parser.parsesSubclasses()) {
        // Ensure there is no conflicting base parser already registered:
        for (Map.Entry<Class<?>, ValueParser<?>> entry : baseParserMap.entrySet()) {
          final Class<?> registeredClass = entry.getKey();
          final ValueParser<?> existing = entry.getValue();
          if (registeredClass.equals(klass)) {
            throw new RuntimeException(String.format(
                "Conflicting argument parsers for subclasses of '%s' : '%s' and '%s'.",
                klass.getName(),
                existing.getClass().getName(),
                parser.getClass().getName()));
          }
          if (registeredClass.isAssignableFrom(klass) || klass.isAssignableFrom(registeredClass)) {
            throw new RuntimeException(String.format(
                "Conflicting argument parsers for subclasses of '%s' : '%s' and '%s'.",
                klass.getName(),
                existing.getClass().getName(),
                parser.getClass().getName()));
          }
        }
        final ValueParser<?> existing = baseParserMap.put(klass, parser);

        // Given the previous checks, existing must be null:
        Preconditions.checkState(existing == null);
      } else {
        final ValueParser<?> existing = parserMap.put(klass, parser);
        if (existing != null) {
          throw new RuntimeException(String.format(
              "Conflicting argument parsers for type '%s' : '%s' and '%s'.",
              klass.getName(),
              existing.getClass().getName(),
              parser.getClass().getName()));
        }
      }
    }
  }
}
