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
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides a static method init(), used to parse params from a context.
 */
public final class ParamParser {

  /** Should not be constructed. */
  private ParamParser() {}

  /**
   * Pulls out all the fields that have been annotated with {@code Param} attributes.
   *
   * @param obj The object containing the param definitions.
   *
   * @return A map from param name to its definition.
   */
  public static Map<String, ParamSpec> extractParamDeclarations(Object obj) {
    final Map<String, ParamSpec> params = new TreeMap<String, ParamSpec>();
    // Walk up the chain of inheritance:
    for (Class<?> clazz = obj.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      // Register public fields with @Param annotations:
      for (Field field : clazz.getDeclaredFields()) {
        final Param param = field.getAnnotation(Param.class);
        if (param != null) {
          final ParamSpec paramSpec = new ParamSpec(field, param, obj);
          final String paramName = paramSpec.getName();
          if (params.containsKey(paramName)) {
            throw new RuntimeException(paramName);
          }
          params.put(paramSpec.getName(), paramSpec);
        }
      }
    }
    return params;
  }
}
