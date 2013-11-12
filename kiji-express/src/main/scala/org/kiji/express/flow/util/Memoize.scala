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

package org.kiji.express.flow.util

import scala.collection.mutable
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * Class used to wrap a function with a single parameter, caching parameter/return value
 * combinations. The wrapped function will only execute when encountering new parameters. This
 * should only be used to wrap pure functions.
 *
 * For example, lets define a function that has a potentially long execution time and memoize it:
 * {{{
 * // A slow function:
 * def factorial(n: Int): Int = {
 *   if (n == 0) {
 *     1
 *   } else {
 *     n * factorial(n - 1)
 *   }
 * }
 *
 * // This will be very fast for each cached input.
 * val cachedFactorial = Memoize(factorial)
 *
 * // This will take some time to run
 * cachedFactorial(100)
 * // This will be very fast now that the factorial of 100 has been cached already.
 * cachedFactorial(100)
 * }}}
 *
 * Note: Functions with multiple parameters can also be used with Memoize by first converting them
 * to a single parameter function where the parameter to the function is a tuple containing the
 * original parameters. This can be achieved with the scala standard library's `tupled` method on
 * functions.
 *
 * @param f is the function to wrap.
 * @tparam T is the input type of the wrapped function.
 * @tparam R is the return type of the wrapped function.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
class Memoize[-T, +R](f: T => R) extends (T => R) {
  /** Mutable map used to store already computed parameter/return value combinations. */
  private[this] val cache = mutable.Map.empty[T, R]

  /**
   * Fetches the return value associated with the provided parameter. This function will return a
   * cached value first if possible.
   *
   * @param x is the parameter to the function.
   * @return the value associated with the provided parameter.
   */
  def apply(x: T): R = {
    // Check to see if 'x' is already in the cache.
    if (cache.contains(x)) {
      // Return the cached value.
      cache(x)
    } else {
      // Compute the return value associated with 'x', add it to the cache, and return it.
      val y = f(x)
      cache += ((x, y))
      y
    }
  }
}

/**
 * Companion object for memoization wrapper that contains factory methods.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
object Memoize {
  /**
   * Wraps a function with Memoize.
   *
   * @param f is a function to wrap.
   * @return the wrapped function that will cache parameter/return value pairs.
   */
  def apply[T, R](f: T => R): Memoize[T, R] = new Memoize(f)
}
