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

package org.kiji.commons.scala

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.commons.ReferenceCountable

/**
 * The Resources object contains various convenience functions while dealing with
 * resources such as Kiji instances or Kiji tables, particularly around releasing
 * or closing them and handling exceptions.
 */
@ApiAudience.Public
@ApiStability.Stable
object ResourceUtils {
  /**
   * Exception that contains multiple exceptions. Typically used in the case where
   * the user gets an exception within their function and further gets an exception
   * during cleanup in finally.
   *
   * @param msg is the message to include with the exception.
   * @param errors causing this exception.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  @Inheritance.Sealed
  final case class CompoundException(msg: String, errors: Seq[Exception]) extends Exception

  /**
   * Performs an operation with a resource that requires post processing. This method will throw a
   * [[CompoundException]] when exceptions get thrown during the operation and while resources are
   * being closed.
   *
   * @tparam T is the return type of the operation.
   * @tparam R is the type of resource such as a Kiji instance or table.
   * @param resource required by the operation.
   * @param after is a function for any post processing on the resource, such as close or release.
   * @param fn is the operation to perform using the resource, like getting the layout of a table.
   * @return the result of the operation.
   * @throws CompoundException if your function crashes as well as the close operation.
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
      case exn: Exception => {
        error = Some(exn)
        throw exn
      }
    } finally {
      try {
        // Cleanup resources.
        after(res)
      } catch {
        // Throw the exception(s).
        case exn: Exception => {
          error match {
            case Some(firstErr) => {
              throw CompoundException(
                  "Exception was thrown while cleaning up resources "
                  + "after another exception was thrown.",
                  Seq(firstErr, exn))
            }
            case None => throw exn
          }
        }
      }
    }
  }

  /**
   * Performs an operation with a releaseable resource by first retaining the resource and releasing
   * it upon completion of the operation.
   *
   * @tparam T is the return type of the operation.
   * @tparam R is the type of resource, such as a Kiji table or instance.
   * @param resource is the retainable resource object used by the operation.
   * @param fn is the operation to perform using the releasable resource.
   * @return the result of the operation.
   */
  def retainAnd[T, R <: ReferenceCountable[R]](
      resource: => ReferenceCountable[R]
  )(fn: R => T): T = {
    doAndRelease[T, R](resource.retain())(fn)
  }

  /**
   * Performs an operation with an already retained releaseable resource releasing it upon
   * completion of the operation.
   *
   * @tparam T is the return type of the operation.
   * @tparam R is the type of resource, such as a Kiji table or instance.
   * @param resource is the retainable resource object used by the operation.
   * @param fn is the operation to perform using the resource.
   * @return the result of the operation.
   */
  def doAndRelease[T, R <: ReferenceCountable[R]](resource: => R)(fn: R => T): T = {
    def after(r: R) { r.release() }
    doAnd(resource, after)(fn)
  }

  /**
   * Performs an operation with a closeable resource closing it upon completion of the operation.
   *
   * @tparam T is the return type of the operation.
   * @tparam C is the type of resource, such as a Kiji table or instance.
   * @param resource is the closeable resource used by the operation.
   * @param fn is the operation to perform using the resource.
   * @return the result of the operation.
   */
  def doAndClose[T, C <: { def close(): Unit }](resource: => C)(fn: C => T): T = {
    def after(c: C) { c.close() }
    doAnd(resource, after)(fn)
  }
}
