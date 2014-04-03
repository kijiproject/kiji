/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.rest.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.scoring.FreshKijiTableReader;

/**
 * A cache object containing all Kiji, KijiTable, and FreshKijiTableReader objects for a Kiji
 * instance. Handles the creation and lifecycle of instances.
 */
public class KijiInstanceCache {

  private static final Logger LOG = LoggerFactory.getLogger(KijiInstanceCache.class);

  private static final long TEN_MINUTES = 10 * 60 * 1000;

  /** Determines whether new values can be loaded into the contained caches. */
  private volatile boolean mIsOpen = true;

  private final Kiji mKiji;

  private final LoadingCache<String, KijiTable> mTables =
      CacheBuilder.newBuilder()
          .removalListener(
              new RemovalListener<String, KijiTable>() {
                @Override
                public void onRemoval(RemovalNotification<String, KijiTable> notification) {
                  try {
                    notification.getValue().release(); // strong cache; should not be null
                  } catch (IOException e) {
                    LOG.warn("Unable to release KijiTable {} with name {}.",
                        notification.getValue(), notification.getValue());
                  }
                }
              }
          )
          .build(
              new CacheLoader<String, KijiTable>() {
                @Override
                public KijiTable load(String table) throws IOException {
                  Preconditions.checkState(mIsOpen, "Cannot open KijiTable in closed cache.");
                  return mKiji.openTable(table);
                }
              }
          );

  private final LoadingCache<String, FreshKijiTableReader> mFreshReaders =
      CacheBuilder.newBuilder()
          .removalListener(
              new RemovalListener<String, FreshKijiTableReader>() {
                @Override
                public void onRemoval(
                    RemovalNotification<String,
                    FreshKijiTableReader> notification
                ) {
                  try {
                    notification.getValue().close(); // strong cache; should not be null
                  } catch (IOException e) {
                    LOG.warn("Unable to close FreshKijiTableReader {} on table {}.",
                        notification.getValue(), notification.getValue());
                  }
                }
              }
          )
          .build(
              new CacheLoader<String, FreshKijiTableReader>() {
                @Override
                public FreshKijiTableReader load(String table) throws IOException {
                  try {
                    Preconditions.checkState(mIsOpen,
                        "Cannot open FreshKijiTableReader in closed cache.");
                    return FreshKijiTableReader.Builder.create()
                        .withTable(mTables.get(table))
                        .withAutomaticReread(TEN_MINUTES)
                        .withPartialFreshening(false)
                        .build();
                  } catch (ExecutionException e) {
                    // Unwrap (if possible) and rethrow. Will be caught by #getFreshKijiTableReader.
                    if (e.getCause() instanceof IOException) {
                      throw (IOException) e.getCause();
                    } else {
                      throw new IOException(e.getCause());
                    }
                  }
                }
              }
          );

  /**
   *
   * Create a new KijiInstanceCache which caches the instance at the provided URI.
   *
   * @param uri of instance to cache access to.
   * @throws IOException if error while opening kiji.
   */
  public KijiInstanceCache(KijiURI uri) throws IOException {
    mKiji = Kiji.Factory.open(uri);
  }

  /**
   * Returns the Kiji instance held by this cache.  This Kiji instance should *NOT* be released.
   *
   * @return a Kiji instance.
   */
  public Kiji getKiji() {
    return mKiji;
  }

  /**
   * Returns the KijiTable instance for the table name held by this cache.  This KijiTable instance
   * should *NOT* be released.
   *
   * @param table name.
   * @return the KijiTable instance
   * @throws java.util.concurrent.ExecutionException if the table cannot be created.
   */
  public KijiTable getKijiTable(String table) throws ExecutionException {
    return mTables.get(table);
  }

  /**
   * Returns the FreshKijiTableReader instance for the table held by this cache.  This
   * FreshKijiTableReader instance should *NOT* be closed.
   *
   * @param table name.
   * @return a FreshKijiTableReader for the table.
   * @throws ExecutionException if a FreshKijiTableReader cannot be created for the table.
   */
  public FreshKijiTableReader getFreshKijiTableReader(String table) throws ExecutionException {
    return mFreshReaders.get(table);
  }

  /**
   * Invalidates cached KijiTable and KijiFreshTableReader instances for a table.
   *
   * @param table name to be invalidated.
   */
  public void invalidateTable(String table) {
    mTables.invalidate(table);
    mFreshReaders.invalidate(table);
  }

  /**
   * Stop creating resources to cache, and cleanup any existing resources.
   *
   * @throws IOException if error while closing instance.
   */
  public void stop() throws IOException {
    mIsOpen = false; // Stop caches from loading more entries
    mFreshReaders.invalidateAll();
    mFreshReaders.cleanUp();
    mTables.invalidateAll();
    mTables.cleanUp();
    mKiji.release();
  }

  /**
   * Checks the health of this KijiInstanceCache, and all the stateful objects it contains.
   *
   * @return a list health issues.  Will be empty if the cache is healthy.
   */
  public List<String> checkHealth() {
    ImmutableList.Builder<String> issues = ImmutableList.builder();
    if (mIsOpen) {

      // Check that the Kiji instance is healthy
      try {
        mKiji.getMetaTable();
      } catch (IllegalStateException e) {
        issues.add(String.format("Kiji instance %s is in illegal state.", mKiji));
      } catch (IOException e) {
        issues.add(String.format("Kiji instance %s cannot open meta table.", mKiji));
      }

      // Check that the KijiTable instances are healthy
      for (KijiTable table : mTables.asMap().values()) {
        try {
          table.getWriterFactory();
        } catch (IllegalStateException e) {
          issues.add(String.format("KijiTable instance %s is in illegal state.", table));
        } catch (IOException e) {
          issues.add(String.format("KijiTable instance %s cannot open reader factory.", table));
        }
      }

      // Check that the FreshKijiTableReader instances are healthy
      for (FreshKijiTableReader freshReader : mFreshReaders.asMap().values()) {
        try {
          freshReader.get(HBaseEntityId.fromHBaseRowKey(new byte[0]), KijiDataRequest.empty());
        } catch (IllegalStateException e) {
          issues.add(String.format("FreshKijiTableReader instance %s is in illegal state.",
              freshReader));
        } catch (IOException e) {
          issues.add(String.format("FreshKijiTableReader instance %s cannot get data.",
              freshReader));
        }
      }
    } else {
      issues.add(String.format("KijiInstanceCache for kiji instance %s is not open.",
          mKiji.getURI().getInstance()));
    }
    return issues.build();
  }
}
