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

package org.kiji.hive.utils;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.KijiRowExpression;
import org.kiji.schema.KijiDataRequest;

/**
 * Creates the data request required for the hive query to execute.
 */
public final class DataRequestOptimizer {
  private static final Logger LOG = LoggerFactory.getLogger(DataRequestOptimizer.class);

  /** Utility class cannot be instantiated. */
  private DataRequestOptimizer() {}

  //TODO make this a singleton

  /**
   * Constructs the data request required to read the data in the given expressions.
   *
   * @param expressions The Kiji row expressions describing the data to read.
   * @return The data request.
   */
  public static KijiDataRequest getDataRequest(List<KijiRowExpression> expressions) {
    // TODO: Use only the expressions that are used in the current query.

    // TODO: Don't request all versions at all timestamps if we don't have to.
    KijiDataRequest merged = KijiDataRequest.builder().build();

    //TODO Rewrite this to use new builder semantics.
    for (KijiRowExpression expression : expressions) {
      merged = merged.merge(expression.getDataRequest());
    }

    // If this is a * build an expression that includes everything
    return merged;
  }
}
