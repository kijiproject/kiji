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

package org.kiji.schema.shell.ddl.key

import scala.collection.JavaConversions._
import scala.collection.mutable.Set

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.RowKeyFormat2
import org.kiji.schema.shell.DDLException

/**
 * A parameter to a FormattedKeySpec specifying the hash behavior
 * of the key, based on its own nested set of FormattedKeyHashParams.
 *
 * @param hashParams a list of FormattedKeyHashParam properties that specify the
 *     hashing behavior.
 */
@ApiAudience.Private
final class KeyHashParams(val hashParams: List[FormattedKeyHashParam]) extends FormattedKeyParam {

  /**
   * Validates the key hash parameters.
   *
   * @param names the names of the declared key components.
   * @throws DDLException if there's a misconfigured parameter.
   */
  def validate(names: Set[String]): Unit = {
    var seenSize = false
    var seenThrough = false
    var seenSuppress = false

    hashParams.foreach { param =>
      param.validate(names)
      param match {
        case _: FormattedKeyHashSize => {
          if (seenSize) {
            throw new DDLException("HASH(SIZE) argument specified multiple times.")
          }
          seenSize = true
        }
        case _: FormattedKeyHashComponent => {
          if (seenThrough) {
            throw new DDLException("HASH(THROUGH) argument specified multiple times.")
          }
          seenThrough = true
        }
        case _: FormattedKeySuppressFields => {
          if (seenSuppress) {
            throw new DDLException("HASH(SUPPRESS FIELDS) argument specified multiple times.")
          }
          seenSuppress = true
        }
      }
    }
  }

  /**
   * Configure hashing properties of a RowKeyFormat2 under construction.
   *
   * @param format the RowKeyFormat2 builder to modify with these hashing settings.
   * @return the same RowKeyFormat2 builder.
   */
  def configureHashing(format: RowKeyFormat2.Builder): RowKeyFormat2.Builder = {
    hashParams.foreach { param =>
      param.updateHashProperties(format)
    }
    return format
  }

  /**
   * If the user has specified a SIZE parameter, return its value here.
   *
   * @return Some(size) where 'size' is the hash size in bytes, if configured; None otherwise.
   */
  def getSpecifiedHashSize(): Option[Int] = {
    hashParams.foreach { param =>
      param match {
        case sizeParam: FormattedKeyHashSize => {
          return Some(sizeParam.size)
        }
        case _ => { }
      }
    }

    return None
  }

  /**
   * If the user specified a THROUGH parameter, return its value here.
   *
   * @param format the RowKeyFormat2 builder being used to construct the key spec.
   * @return Some(thru) where 'thru' is the index of the last element to include in the hash,
   *     if configured; None otherwise.
   */
  def getSpecifiedHashThruIdx(format: RowKeyFormat2.Builder): Option[Int] = {
    hashParams.foreach { param =>
      param match {
        case thruParam: FormattedKeyHashComponent => {
          format.getComponents().zipWithIndex.foreach { case(component, idx) =>
            if (component.getName() == thruParam.name) {
              return Some(idx)
            }
          }
        }
        case _ => { }
      }
    }

    return None
  }
}
