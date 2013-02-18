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

package org.kiji.mapreduce.impl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.framework.KijiConfKeys;

/** Instantiates a KijiTableContext according to a configuration. */
@ApiAudience.Private
public final class KijiTableContextFactory {

  /**
   * Instantiates the configured KijiTableContext.
   *
   * @param taskContext Hadoop task context.
   * @return the configured KijiTableContext.
   * @throws IOException on I/O error.
   */
  public static KijiTableContext create(TaskInputOutputContext taskContext)
      throws IOException {
    final Configuration conf = taskContext.getConfiguration();
    final String className = conf.get(KijiConfKeys.KIJI_TABLE_CONTEXT_CLASS);
    if (className == null) {
      throw new IOException(String.format(
          "KijiTableContext class missing from configuration (key '%s').",
          KijiConfKeys.KIJI_TABLE_CONTEXT_CLASS));
    }

    Throwable throwable = null;
    try {
      final Class<?> genericClass = Class.forName(className);
      final Class<? extends KijiTableContext> klass =
          genericClass.asSubclass(KijiTableContext.class);
      final Constructor<? extends KijiTableContext> constructor =
          klass.getConstructor(TaskInputOutputContext.class);
      final KijiTableContext context = constructor.newInstance(taskContext);
      return context;
    } catch (ClassCastException cce) {
      throwable = cce;
    } catch (ClassNotFoundException cnfe) {
      throwable = cnfe;
    } catch (NoSuchMethodException nsme) {
      throwable = nsme;
    } catch (InvocationTargetException ite) {
      throwable = ite;
    } catch (IllegalAccessException iae) {
      throwable = iae;
    } catch (InstantiationException ie) {
      throwable = ie;
    }
    throw new IOException(
        String.format("Error instantiating KijiTableWriter class '%s': %s.",
            className, throwable.getMessage()),
        throwable);
  }

  /** Utility class cannot be instantiated. */
  private KijiTableContextFactory() {
  }
}
