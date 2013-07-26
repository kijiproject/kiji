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

import java.util.ArrayList
import scala.collection.mutable.HashSet

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.RowKeyComponent
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.RowKeyFormat2
import org.kiji.schema.shell.DDLException

/**
 * Specification of a FORMATTED row key. Contains an ordered list of FormattedKeyParam
 * arguments that specify either the name and type of another component in the key,
 * or properties that control how it is hashed.
 *
 * @param params the list of parameters that specify the key format.
 */
@ApiAudience.Private
class FormattedKeySpec(val params: List[FormattedKeyParam]) {

  /**
   * Validates arguments of FormattedKeySpec.
   *
   * @throws DDLException if there is a problem.
   */
  def validate(): Unit = {
    // true if we ever see a KeyHashParams in the FormattedKeyParam list.
    var seenHashParam = false
    val names = HashSet[String]()

    // Current KeyComponent index we're processing.
    var curIndex = 0

    // Index of the left-most element that may be null.
    var nullIndex = 1

    params.foreach { param =>
      param match {
        case hash: KeyHashParams => {
          // Check to make sure we have at most one HASH clause.
          if (seenHashParam) {
            throw new DDLException("Only at most one HASH clause is allowed")
          }
          seenHashParam = true
          hash.validate(names)
        }

        case keyComponent: KeyComponent => {
          // This represents another key component.
          // Check that this name doesn't already exist.
          names.foreach { name =>
            if (name == keyComponent.name) {
              throw new DDLException("Cannot use key component name '" + keyComponent.name +
                  "' more than once.")
            }
          }

          // Add it to the list we've seen.
          names.add(keyComponent.name)

          if (!keyComponent.mayBeNull) {
            if (0 == curIndex) {
              // Ignore this; first element isn't allowed to be null in any case,
              // so we don't require the user type NOT NULL here.
            } else if (nullIndex == curIndex) {
              // Move the null index one to the right.
              nullIndex = nullIndex + 1
            } else {
              throw new DDLException("Row key may not have a NOT NULL component " +
                  "after a nullable one.")
            }
          }

          curIndex = curIndex + 1
        }
        case _ => {
          // Shouldn't get here.
          throw new DDLException("Unexpected typecase in FormattedKeySpec")
        }
      }
    }

    if (names.size < 1) {
      throw new DDLException("Must specify at least one component in a row key")
    }
  }

  def createFormattedKey(): RowKeyFormat2 = {
    val format = RowKeyFormat2.newBuilder()
    val components = new ArrayList[RowKeyComponent]()
    format.setComponents(components)
    format.setEncoding(RowKeyEncoding.FORMATTED)
    format.setSalt(HashSpec.newBuilder().build()) // Initialize with default salting.

    var specifiedHashSize: Option[Int] = None
    var specifiedHashThruIdx: Option[Int] = None

    // Current KeyComponent index we're processing.
    var curIndex = 0

    // Index of the left-most element that may be null.
    var nullIndex = 1

    params.foreach { param =>
      param match {
        case hash: KeyHashParams => {
          specifiedHashSize = hash.getSpecifiedHashSize()
          specifiedHashThruIdx = hash.getSpecifiedHashThruIdx(format)
          hash.configureHashing(format)
        }
        case keyComponent: KeyComponent => {
          // This represents another key component.
          // Add it to the list.
          components.add(keyComponent.toAvro())

          if (!keyComponent.mayBeNull) {
            if (0 == curIndex) {
              // Ignore this; first element isn't allowed to be null in any case,
              // so we don't require the user type NOT NULL here.
            } else if (nullIndex == curIndex) {
              // Move the null index one to the right.
              nullIndex = nullIndex + 1
            }
          }

          curIndex = curIndex + 1
        }
      }
    }

    format.setNullableStartIndex(nullIndex)

    // If the hash spec doesn't contain a SIZE param, and we've disabled materialization,
    // set the size of the hash to 16. If materialization is disabled, we must also explicitly
    // specify that all fields are part of the hashed side of the key.
    if (format.getSalt().getSuppressKeyMaterialization()) {
      specifiedHashThruIdx match {
        case Some(thruIdx) => {
          // Double-check that thruIdx is the index of the last component.
          // When key materialization is suppressed, the user must hash all
          // components explicitly.
          if (thruIdx != format.getComponents().size - 1) {
            throw new DDLException("If SUPPRESS FIELDS is specified, you must hash all "
                + "components. If you omit the THROUGH clause, this is handled for you.")
          }
        }
        case None => { format.setRangeScanStartIndex(format.getComponents().size) }
      }

      specifiedHashSize match {
        case Some(size) => { /* Do nothing. */ }
        case None => { format.getSalt().setHashSize(16) /* max size hash. */ }
      }
    } else {
      specifiedHashSize match {
        case Some(size) => { /* Do nothing. */ }
        case None => { format.getSalt().setHashSize(2) /** Reasonable hash prefix. */ }
      }
    }

    format.build()
  }
}

/**
 * A FormattedKeySpec that represents ROW KEY FORMAT RAW.
 */
object RawFormattedKeySpec extends FormattedKeySpec(List()) {

  override def validate(): Unit = {
    // no validation required in RAW key. Other FORMATTED validation is inappropriate.
  }

  override def createFormattedKey(): RowKeyFormat2 = {
    // Set the encoding to RAW. No other fields are needed.
    return RowKeyFormat2.newBuilder()
        .setSalt(null)
        .setEncoding(RowKeyEncoding.RAW).build()
  }
}

/** A FormattedKeySpec that represents ROW KEY FORMAT HASHED. */
final class HashedFormattedKeySpec extends FormattedKeySpec(List(
  new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
  new KeyHashParams(List(
    new FormattedKeyHashSize(16),
    new FormattedKeySuppressFields
  ))
))

/**
 * A FormattedKeySpec that represents ROW KEY FORMAT HASH PREFIXED(n).
 *
 * @param size the size of the hash prefix in bytes.
 */
class HashPrefixKeySpec(val size: Int) extends FormattedKeySpec(List(
  new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
  new KeyHashParams(List(
    new FormattedKeyHashSize(size)
  ))
)) {

  /** ROW KEY FORMAT HASH PREFIXED(n) must explicitly check that n &gt; 0. */
  override def validate(): Unit = {
    if (size < 1) {
      throw new DDLException("HASH PREFIXED requires hash size > 0.")
    }
    super.validate()
  }
}

/** The default key spec, if the user doesn't specify one, is HASH PREFIXED(2). */
object DefaultKeySpec extends HashPrefixKeySpec(2)
