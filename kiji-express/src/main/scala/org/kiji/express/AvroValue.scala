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

package org.kiji.express

import org.apache.avro.specific.SpecificRecord

import org.kiji.annotations.Inheritance
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * An class which every AvroValue originally derived from an AvroRecord inherits from.  It defines
 * the methods for retrieving the contained object of various types, with default implementations
 * that throw `UnsupportedOperationException`.  Each implementing class implements the appropriate
 * method given the type of the class.
 *
 * See inheriting classes defined in AvroValueImpls.scala.
 *
 * @param classOfValue is the class of the value this wraps, for more informative error messages.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
abstract class AvroValue private[express](classOfValue: Class[_]) {
  /** An error message used for primitives being cast to the wrong types. */
  private val castErrorMessage: String =
      "This AvroValue is of type %s".format(classOfValue.getName) + ", not of type %s."

  /**
   * Gets this AvroValue as an Int, if possible.
   *
   * @return this AvroValue as an Int, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an Int.
   */
  def asInt(): Int = {
    throw new UnsupportedOperationException(castErrorMessage.format("Int"))
  }

  /**
   * Gets this AvroValue as an Boolean, if possible.
   *
   * @return this AvroValue as a Boolean, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an Boolean.
   */
  def asBoolean(): Boolean = {
    throw new UnsupportedOperationException(castErrorMessage.format("Boolean"))
  }

  /**
   * Gets this AvroValue as a Long, if possible.
   *
   * @return this AvroValue as a Long, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an Long.
   */
  def asLong(): Long = {
    throw new UnsupportedOperationException(castErrorMessage.format("Long"))
  }

  /**
   * Gets this AvroValue as a Float, if possible.
   *
   * @return this AvroValue as a Float, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an Float.
   */
  def asFloat(): Float = {
    throw new UnsupportedOperationException(castErrorMessage.format("Float"))
  }

  /**
   * Gets this AvroValue as a Double, if possible.
   *
   * @return this AvroValue as a Double, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an Double.
   */
  def asDouble(): Double = {
    throw new UnsupportedOperationException(castErrorMessage.format("Double"))
  }

  /**
   * Gets this AvroValue as a String, if possible.
   *
   * @return this AvroValue as a String, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an String.
   */
  def asString(): String = {
    throw new UnsupportedOperationException(castErrorMessage.format("String"))
  }

  /**
   * Gets this AvroValue as a List of AvroValues, if possible.
   *
   * @return this AvroValue as a List of AvroValues, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an List.
   */
  def asList(): List[AvroValue] = {
    throw new UnsupportedOperationException(castErrorMessage.format("List"))
  }

  /**
   * Gets this AvroValue as a Map, if possible.
   *
   * @return this AvroValue as a Map, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an Map.
   */
  def asMap(): Map[String, AvroValue] = {
    throw new UnsupportedOperationException(castErrorMessage.format("Map"))
  }

  /**
   * Gets this AvroValue as a byte array, if possible.
   *
   * @return this AvroValue as a byte array, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not a byte array.
   */
  def asBytes(): Array[Byte] = {
    throw new UnsupportedOperationException(castErrorMessage.format("byte array"))
  }

  /**
   * Gets this AvroValue as an AvroRecord, if possible.
   *
   * @return this AvroValue as an AvroRecord, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an AvroRecord.
   */
  def asRecord(): AvroRecord = {
    throw new UnsupportedOperationException(castErrorMessage.format("Generic Record"))
  }

  /**
   * If this AvroValue is an AvroEnum, gets the string name of its value.
   *
   * @return this AvroValue as an AvroEnum, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not an Enum.
   */
  def asEnumName(): String = {
    throw new UnsupportedOperationException(castErrorMessage.format("Enum"))
  }

  /**
   * If this AvroValue is an AvroFixed, gets the string name of its value.
   *
   * @return this AvroValue as a fixed-length byte array, if possible.
   * @throws UnsupportedOperationException if this AvroValue is not a Fixed.
   */
  def asFixedBytes(): Array[Byte] = {
    throw new UnsupportedOperationException(castErrorMessage.format("Fixed"))
  }

  /**
   * Gets this AvroValue as a SpecificRecord if possible.
   *
   * @tparam T is the type of the SpecificRecord class contained in this AvroValue.
   * @return this AvroValue as a SpecificRecord if possible.
   * @throws UnsupportedOperationException if this AvroValue is not a SpecificRecord.
   */
  def asSpecificRecord[T <: SpecificRecord](): T = {
    throw new UnsupportedOperationException(castErrorMessage.format("Specific Record"))
  }

  /**
   * Accesses a field in this AvroValue, if this is an AvroRecord, or value in this map, if this
   * is an AvroMap.
   *
   * @param recordFieldOrMapKey to access.
   * @return the field in this AvroRecord or value in this AvroMap, if possible.
   * @throws UnsupportedOperationException if this is a primitive type or list, not a record.
   */
  def apply(recordFieldOrMapKey: String): AvroValue = {
    val errorMessage =
        "This AvroValue is a %s, not a Record or Map; it does not have fields to access."
        .format(classOfValue.getName)
    throw new UnsupportedOperationException(errorMessage)
  }

  /**
   * Accesses an element in this AvroValue, if this is an AvroList.
   *
   * @param index to access.
   * @return the index'th element of this list.
   * @throws UnsupportedOperationException if this is not a list.
   */
  def apply(index: Int): AvroValue = {
    val errorMessage = "This AvroValue is a %s, not a list; you cannot access it by index."
        .format(classOfValue.getName)
    throw new UnsupportedOperationException(errorMessage)
  }
}
