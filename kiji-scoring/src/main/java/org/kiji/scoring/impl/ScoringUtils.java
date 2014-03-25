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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Maps;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderPool;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;

/** Utility class for commonly used methods in KijiScoring. */
@ApiAudience.Private
public final class ScoringUtils {

  /** Utility classes may not be instantiated. */
  private ScoringUtils() { }

  /**
   * Create a new instance of the named ScoreFunction subclass.
   *
   * @param scoreFunctionClassName fully qualified class name of the ScoreFunction subclass to
   *     instantiate.
   * @return a new instance of the named ScoreFunction subclass.
   */
  public static ScoreFunction<?> scoreFunctionForName(
      final String scoreFunctionClassName
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(scoreFunctionClassName).asSubclass(ScoreFunction.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  /**
   * Create a new instance of the named KijiFreshnessPolicy subclass.
   *
   * @param policyClassName fully qualified class name of the KijiFreshnessPolicy subclass to
   *     instantiate.
   * @return a new instance of the named KijiFreshnessPolicy subclass.
   */
  public static KijiFreshnessPolicy policyForName(
      final String policyClassName
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(policyClassName).asSubclass(KijiFreshnessPolicy.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  /**
   * Create a KeyValueStoreReaderFactory from the required stores of a ScoreFunction and
   * KijiFreshnessPolicy. Stores defined by the policy override those defined by the ScoreFunction.
   *
   * @param context FreshenerGetStoresContext with which to call getRequiredStores(context).
   * @param scoreFunction ScoreFunction from which to get required stores.
   * @param policy KijiFreshnessPolicy from which to get required stores.
   * @return a new KeyValueStoreReaderFactory configured to read the required stores of the given
   *     ScoreFunction and KijiFreshnessPolicy.
   */
  public static KeyValueStoreReaderFactory createKVStoreReaderFactory(
      final InternalFreshenerContext context,
      final ScoreFunction<?> scoreFunction,
      final KijiFreshnessPolicy policy
  ) {
    final Map<String, KeyValueStore<?, ?>> kvMap = Maps.newHashMap();
    kvMap.putAll(scoreFunction.getRequiredStores(context));
    kvMap.putAll(policy.getRequiredStores(context));
    return KeyValueStoreReaderFactory.create(kvMap);
  }

  /**
   * Get a future from a given callable.  This method uses the singleton FreshenerThreadPool to run
   * threads responsible for carrying out the operation of the Future.
   *
   * @param executorService ExecutorService to use to get the Future.
   * @param callable the callable to run in the new Future.
   * @param <RETVAL> the return type of the callable and Future.
   * @return a new Future representing asynchronous execution of the given callable.
   */
  public static <RETVAL> Future<RETVAL> getFuture(
      ExecutorService executorService,
      Callable<RETVAL> callable
  ) {
    return executorService.submit(callable);
  }

  /**
   * Get the value from a given Future.  This blocks until the Future is complete.
   *
   * @param future the Future from which to get the resultant value.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   */
  public static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future
  ) {
    try {
      return future.get();
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * Get the value from a given Future with a timeout.  This blocks until the Future is complete or
   * the timeout expires.
   *
   * @param future the Future from which to get the resultant value.
   * @param timeout the time to wait (in milliseconds) before a TimeoutException.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   * @throws TimeoutException if the Future does not return before the timeout period elapses.
   */
  public static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future,
      final long timeout
  ) throws TimeoutException {
    return getFromFuture(future, timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the value from a given Future with a timeout.  This blocks until the Future is complete or
   * the timeout expires.
   *
   * @param future the Future from which to get the resultant value.
   * @param timeout the time to wait (in units defined by timeUnit) before a TimeoutException.
   * @param timeUnit the unit of time to use for the timeout.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   * @throws TimeoutException if the Future does not return before the timeout period elapses.
   */
  public static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future,
      final long timeout,
      final TimeUnit timeUnit
  ) throws TimeoutException {
    try {
      return future.get(timeout, timeUnit);
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * Get a KijiTableReader from the given reader pool.
   *
   * @param pool KijiTableReaderPool from which to get a reader.
   * @return a KijiTableReader from the given pool.
   * @throws java.io.IOException in case of an error borrowing a reader from the pool.
   */
  public static KijiTableReader getPooledReader(
      final KijiTableReaderPool pool
  ) throws IOException {
    try {
      return pool.borrowObject();
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }
}
