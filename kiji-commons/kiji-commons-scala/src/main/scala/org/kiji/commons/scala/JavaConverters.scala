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

import com.google.common.base.Optional

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/** Implicit converters between Java and Scala idioms. */
@ApiAudience.Framework
@ApiStability.Experimental
object JavaConverters {

  /**
   * Implicit converter between scala Option and guava Optional.
   *
   * @param opt Option to convert to an Optional.
   * @tparam T Type of the value held in the Option.
   */
  implicit class OptionAsOptional[T](
      opt: Option[T]
  ) {
    def asJava: Optional[T] = {
      opt match {
        case Some(x) => {
          // Because Some may be explicitly created with a null value, we must manually check if the
          // value is null.
          if (x == null) {
            Optional.absent()
          } else {
            Optional.of(x)
          }
        }
        case None => Optional.absent()
      }
    }
  }

  /**
   * Implicit converter between guava Optional and scala Option.
   *
   * @param opt Optional to convert to an Option.
   * @tparam T Type of the value held in the Optional.
   */
  implicit class OptionalAsOption[T](
      opt: Optional[T]
  ) {
    def asScala: Option[T] = Option(opt.orNull())
  }
}
