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

package org.kiji.scoring;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiTable;
import org.kiji.scoring.impl.InternalFreshKijiTableReader;

/**
 * Factory for FreshKijiTableReader instances.
 * Usage:
 * <p>
 *  <code>
 *  FreshKijiTableReaderFactory.getFactory(FreshReaderFactoryType.LOCAL).openReader(table, timeout);
 *  </code>
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public abstract class FreshKijiTableReaderFactory {

  /** Enumeration of types of fresh reader factories. */
  public static enum FreshReaderFactoryType {
    LOCAL
  }

  /** A default factory to provide a convenient way to get readers for the common case. */
  private static final FreshKijiTableReaderFactory DEFAULT_FACTORY =
      new LocalFreshKijiTableReaderFactory();

  /**
   * Get a FreshKijiTableReaderFactory for the specified type of FreshReader.
   *
   * @param type the type of FreshKijiTableReaderFactory to return.
   * @return a FreshKijiTableReaderFactory of the specified type.
   */
  public static FreshKijiTableReaderFactory getFactory(FreshReaderFactoryType type) {
    switch (type) {
      case LOCAL:
        return new LocalFreshKijiTableReaderFactory();
      default:
        throw new InternalKijiError(
            String.format("Unknown FreshReaderFactoryType: %s", type.toString()));
    }
  }

  /**
   * Get the default FreshKijiTableReaderFactory instance, provided for convenience.
   *
   * @return the default FreshKijiTableReaderFactory instance.
   */
  public static FreshKijiTableReaderFactory getDefaultFactory() {
    return DEFAULT_FACTORY;
  }

  /**
   * Open a new FreshKijiTableReader for a given table with a given read timeout.  This reader will
   * not automatically reload freshness policy data from the metatable.
   *
   * @param table the KijiTable to read from.
   * @param timeout how long to wait before returning stale data.
   * @return a new FreshKijiTableReader for a given table with a given read timeout.
   * @throws java.io.IOException in case of an error opening the reader.
   */
  public abstract FreshKijiTableReader openReader(KijiTable table, int timeout) throws IOException;

  /**
   * Open a new FreshKijiTableReader for a given table with a given read timeout.  This reader will
   * automatically reload freshness policy data from the metatable periodically.
   *
   * @param table the KijiTable to read from.
   * @param timeout how long to wait before returning stale data.
   * @param reloadTime how long between automatically reloading freshness policies from the
   * metatable.
   * @return a new FreshKijiTableReader for a given table with a given read timeout and reload time.
   * @throws IOException in case of an error opening the reader.
   */
  public abstract FreshKijiTableReader openReader(KijiTable table, int timeout, int reloadTime)
      throws IOException;

  /**
   * FreshKijiTableReaderFactory for creating InternalFreshKijiTableReader instances.
   */
  public static final class LocalFreshKijiTableReaderFactory extends FreshKijiTableReaderFactory{

    /** {@inheritDoc} */
    @Override
    public FreshKijiTableReader openReader(KijiTable table, int timeout) throws IOException {
      return openReader(table, timeout, 0);
    }

    /** {@inheritDoc} */
    @Override
    public FreshKijiTableReader openReader(KijiTable table, int timeout, int reloadTime)
        throws IOException {
      return new InternalFreshKijiTableReader(table, timeout, reloadTime);
    }
  }

}
