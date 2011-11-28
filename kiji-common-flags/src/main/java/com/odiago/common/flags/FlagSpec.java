/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
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

package com.odiago.common.flags;

import java.lang.reflect.Field;

/**
 * Everything there is to know about a flag definition.
 */
class FlagSpec {
  /** The Java field that should be assigned to with the flag's value. */
  private Field mField;

  /** The Flag annotation. */
  private Flag mFlag;

  /** The object that contains the flag definition. */
  private Object mObj;

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
      if (mField.getType() == boolean.class) {
        return Boolean.toString(mField.getBoolean(mObj));
      }
      if (mField.getType() == double.class) {
        return Double.toString(mField.getDouble(mObj));
      }
      if (mField.getType() == float.class) {
        return Float.toString(mField.getFloat(mObj));
      }
      if (mField.getType() == int.class) {
        return Integer.toString(mField.getInt(mObj));
      }
      if (mField.getType() == long.class) {
        return Long.toString(mField.getLong(mObj));
      }
      if (mField.getType() == short.class) {
        return Short.toString(mField.getShort(mObj));
      }
      if (mField.getType() == String.class) {
        String s = (String) mField.get(mObj);
        if (s != null) {
          return "\"" + s + "\"";
        }
        return "null";
      }
      return "";
    } catch (IllegalAccessException e) {
      throw new IllegalAccessError(e.getMessage());
    }
  }

  /**
   * Assigns the value of the flag to its field.
   *
   * @param value The value.
   *
   * @throws IllegalAccessException If the field cannot be assigned.
   */
  public void setValue(String value) throws IllegalAccessException {
    if (!mField.isAccessible()) {
      mField.setAccessible(true);
    }
    if (mField.getType() == boolean.class) {
      if (value.equals("true") || value.isEmpty()) {
        mField.setBoolean(mObj, true);
      } else if (value.equals("false")) {
        mField.setBoolean(mObj, false);
      } else {
        throw new IllegalFlagValueException(getName(), value);
      }
    } else if (mField.getType() == double.class) {
      try {
        mField.setDouble(mObj, Double.parseDouble(value));
      } catch (NumberFormatException e) {
        throw new IllegalFlagValueException(getName(), value);
      }
    } else if (mField.getType() == float.class) {
      try {
        mField.setFloat(mObj, Float.parseFloat(value));
      } catch (NumberFormatException e) {
        throw new IllegalFlagValueException(getName(), value);
      }
    } else if (mField.getType() == int.class) {
      try {
        mField.setInt(mObj, Integer.parseInt(value));
      } catch (NumberFormatException e) {
        throw new IllegalFlagValueException(getName(), value);
      }
    } else if (mField.getType() == long.class) {
      try {
        mField.setLong(mObj, Long.parseLong(value));
      } catch (NumberFormatException e) {
        throw new IllegalFlagValueException(getName(), value);
      }
    } else if (mField.getType() == short.class) {
      try {
        mField.setShort(mObj, Short.parseShort(value));
      } catch (NumberFormatException e) {
        throw new IllegalFlagValueException(getName(), value);
      }
    } else if (mField.getType() == String.class) {
      mField.set(mObj, value);
    } else {
      throw new UnsupportedFlagTypeException(mField.getName());
    }
  }
}
