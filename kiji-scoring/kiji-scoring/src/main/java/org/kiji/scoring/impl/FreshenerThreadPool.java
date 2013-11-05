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
package org.kiji.scoring.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.kiji.annotations.ApiAudience;

/**
 * Singleton class providing a cached thread pool for Freshening table reads.
 */
@ApiAudience.Private
public final class FreshenerThreadPool {
  /** Default number of threads. */
  public static final int DEFAULT_THREAD_POOL_SIZE = 100;

  /** Enum used to guarantee that only a single thread pool is created. */
  public enum Singleton {
    GET(Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE));

    private final ExecutorService mExecutorService;

    /**
     * Initialize the singleton.
     *
     * @param executorService the ExecutorService to store in this singleton.
     */
    Singleton(
        final ExecutorService executorService
    ) {
      mExecutorService = executorService;
    }

    /**
     * Get the ExecutorService stored in this singleton.
     *
     * @return the ExecutorService stored in this singleton.
     */
    public ExecutorService getExecutorService() {
      return mExecutorService;
    }
  }

  /** Utility classes may not be instantiated. */
  private FreshenerThreadPool() { }
}
