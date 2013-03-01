/**
 * Licensed to WibiData, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  WibiData, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.kiji.common.flags;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.kiji.delegation.Lookup;
import org.kiji.delegation.Lookups;

/**
 * Everything there is to know about a flag definition.
 */
public class FlagSpec {
  /** The Java field that should be assigned to with the flag's value. */
  private final Field mField;

  /** The Flag annotation. */
  private final Flag mFlag;

  /** The object that contains the flag definition. */
  private final Object mObj;

  /** Parser for this flag. */
  private final ValueParser<?> mParser;

  /**
   * Creates a new <code>FlagSpec</code> instance.
   *
   * @param field The Java field.
   * @param flag The annotation on the field.
   * @param obj The object that contains the field.
   */
  public FlagSpec(Field field, Flag flag, Object obj) {
    mField = field;
    mFlag = flag;
    mObj = obj;
    mParser = findParser();
  }

  /**
   * Identifies the parser for this flag.
   *
   * Requires mField and mFlag being set properly.
   *
   * @return the command-line argument parser for this flag.
   */
  private ValueParser<?> findParser() {
    final Class<?> fieldClass = mField.getType();

    if (mFlag.parser() != ValueParser.class) {
      try {
        final ValueParser<?> parser = mFlag.parser().newInstance();
        final Class<?> parsedClass = parser.getParsedClass();
        if (parser.parsesSubclasses()) {
          if (!parsedClass.isAssignableFrom(fieldClass)) {
            throw new RuntimeException(String.format(
                "Parser '%s' does not match field: %s", parser.getClass().getName(), mField));
          }
        } else {
          if (parsedClass != fieldClass) {
            throw new RuntimeException(String.format(
                "Parser '%s' does not match field type: %s", parser.getClass().getName(), mField));
          }
        }
        return parser;
      } catch (IllegalAccessException iae) {
        throw new RuntimeException(iae);
      } catch (InstantiationException ie) {
        throw new RuntimeException(ie);
      }
    }

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

    // Failed to find a matching flag parser:
    throw new UnsupportedFlagTypeException(this);
  }

  /**
   * Gets the name of the flag.
   *
   * @return The name of the flag.
   */
  public String getName() {
    return mFlag.name().isEmpty() ? mField.getName() : mFlag.name();
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
    if (type == String.class) {
      return "String";
    }
    return type.toString();
  }

  /**
   * Get the human-readable usage instructions for the flag.
   *
   * @return The human-readable usage.
   */
  public String getUsage() {
    return mFlag.usage();
  }

  /**
   * Determines whether the flag should be shown when {@code --help} is provided.
   *
   * @return Whether the flag is hidden.
   */
  public boolean isHidden() {
    return mFlag.hidden();
  }

  /**
   * Gets the name of the environment variable that should be used as the default value.
   *
   * @return The environment variable to use as the default value.
   */
  public String getEnvVar() {
    return mFlag.envVar();
  }

  /**
   * Gets the default value for the flag as a string.
   *
   * @return The default value as a string.
   */
  public String getDefaultValue() {
    try {
      if (!mField.isAccessible()) {
        mField.setAccessible(true);
      }
      return String.format("%s", mField.get(mObj));
    } catch (IllegalAccessException e) {
      throw new IllegalAccessError(e.getMessage());
    }
  }

  /**
   * Assigns the value of the flag to its field.
   *
   * @param values All the values specified for this flag, in the order they are specified.
   * @throws IllegalAccessException If the field cannot be assigned.
   */
  public void setValue(List<String> values) throws IllegalAccessException {
    if (!mField.isAccessible()) {
      mField.setAccessible(true);
    }
    if (values.isEmpty()) {
      if (mFlag.required()) {
        throw new IllegalFlagValueException(String.format(
            "Flag '--%s' is required but has not been specified.", getName()));
      }
    } else {
      mField.set(mObj, mParser.parse(this, values));
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return mField.getDeclaringClass().getName() + "." + mField.getName();
  }

  /**
   * Map from flag value class to flag value parser.
   *
   * The parsers in this map parse exactly the class specified in the key.
   */
  private static final ImmutableMap<Class<?>, ValueParser<?>> PARSERS;

  /**
   * Map from flag value base class to flag value parser.
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
   * Builds the command-line argument parser maps.
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
                "Conflicting command-line argument parsers for subclasses of '%s' : '%s' and '%s'.",
                klass.getName(),
                existing.getClass().getName(),
                parser.getClass().getName()));
          }
          if (registeredClass.isAssignableFrom(klass) || klass.isAssignableFrom(registeredClass)) {
            throw new RuntimeException(String.format(
                "Conflicting command-line argument parsers for subclasses of '%s' : '%s' and '%s'.",
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
              "Conflicting command-line argument parsers for type '%s' : '%s' and '%s'.",
              klass.getName(),
              existing.getClass().getName(),
              parser.getClass().getName()));
        }
      }
    }
  }
}
