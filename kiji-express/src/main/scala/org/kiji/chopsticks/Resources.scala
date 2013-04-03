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

package org.kiji.chopsticks

import java.io.Closeable

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.util.ReferenceCountable

@ApiAudience.Public
@ApiStability.Experimental
object Resources {
  /**
   * Exception that contains multiple exceptions.
   *
   * @param msg Message to include with the exception.
   * @param errors Multiple exceptions causing this exception.
   */
  final case class CompoundException(msg: String, errors: Seq[Exception]) extends Exception

  /**
   * Performs an operation with a resource that requires post processing. This method will throw a
   * [[org.kiji.chopsticks.Resources.CompoundException]] when exceptions get thrown
   * during the operation and while resources are being closed.
   *
   * @tparam T Return type of the operation.
   * @tparam R Type of resource.
   * @param resource Opens the resource required by the operation.
   * @param after Performs any post processing on the resource.
   * @param fn Operation to perform.
   * @return The result of the operation.
   */
  def doAnd[T, R](resource: => R, after: R => Unit)(fn: R => T): T = {
    var error: Option[Exception] = None

    // Build the resource.
    val res: R = resource
    try {
      // Perform the operation.
      fn(res)
    } catch {
      // Store the exception in case close fails.
      case e: Exception => {
        error = Some(e)
        throw e
      }
    } finally {
      try {
        // Cleanup resources.
        after(res)
      } catch {
        // Throw the exception(s).
        case e: Exception => {
          error match {
            case Some(firstErr) => throw CompoundException("Exception was thrown while cleaning up "
                + "resources after another exception was thrown.", Seq(firstErr, e))
            case None => throw e
          }
        }
      }
    }
  }

  /**
   * Performs an operation with a releaseable resource by first retaining the resource and releasing
   * it upon completion of the operation.
   *
   * @tparam T Return type of the operation.
   * @tparam R Type of resource.
   * @param resource Retainable resource used by the operation.
   * @param fn Operation to perform.
   * @return The result of the operation.
   */
  def retainAnd[T, R <: ReferenceCountable[R]](
      resource: => ReferenceCountable[R])(fn: R => T): T = {
    doAndRelease[T, R](resource.retain())(fn)
  }

  /**
   * Performs an operation with an already retained releaseable resource releasing it upon
   * completion of the operation.
   *
   * @tparam T Return type of the operation.
   * @tparam R Type of resource.
   * @param resource Retainable resource used by the operation.
   * @param fn Operation to perform.
   * @return The restult of the operation.
   */
  def doAndRelease[T, R <: ReferenceCountable[R]](resource: => R)(fn: R => T): T = {
    def after(r: R) { r.release() }
    doAnd(resource, after)(fn)
  }

  /**
   * Performs an operation with a closeable resource closing it upon completion of the operation.
   *
   * @tparam T Return type of the operation.
   * @tparam R Type of resource.
   * @param resource Closeable resource used by the operation.
   * @param fn Operation to perform.
   * @return The restult of the operation.
   */
  def doAndClose[T, C <: { def close(): Unit }](resource: => C)(fn: C => T): T = {
    def after(c: C) { c.close() }
    doAnd(resource, after)(fn)
  }
}
