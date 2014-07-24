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

package org.kiji.scoring.params.parser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.Locale;

import com.google.common.base.Joiner;

import org.kiji.scoring.params.IllegalParamValueException;
import org.kiji.scoring.params.ParamSpec;

/**
 * Parser for enum values from context params.
 */
@SuppressWarnings("rawtypes")
public final class EnumParser extends SimpleValueParser<Enum> {
  /** {@inheritDoc} */
  @Override
  public boolean parsesSubclasses() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends Enum> getParsedClass() {
    return Enum.class;
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public Enum parse(ParamSpec param, String value) {
    final Class<? extends Enum> enumClass = param.getType().asSubclass(Enum.class);
    try {
      final Method enumValueOf = enumClass.getMethod("valueOf", String.class);
      return (Enum) enumValueOf.invoke(null, value.toUpperCase(Locale.ROOT));

    } catch (NoSuchMethodException nsme) {
      throw new RuntimeException(nsme);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    } catch (InvocationTargetException ite) {
      throw new IllegalParamValueException(String.format(
          "Invalid %s enum parameter argument '%s=%s': expecting one of %s.",
          enumClass.getSimpleName(), param.getName(), value,
          Joiner.on(',').join(EnumSet.allOf(enumClass))));
    }
  }
}
