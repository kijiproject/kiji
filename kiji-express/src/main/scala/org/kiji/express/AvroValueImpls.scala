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

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.avro.generic.IndexedRecord

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Represents an Int from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroInt private[express](value: Int) extends AvroValue(classOf[Int]) {
  override def asInt(): Int = value
}

/**
 * Represents a Boolean from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroBoolean private[express](value: Boolean) extends AvroValue(classOf[Boolean]) {
  override def asBoolean(): Boolean = value
}

/**
 * Represents a Long from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroLong private[express](value: Long) extends AvroValue(classOf[Long]) {
  override def asLong(): Long = value
}

/**
 * Represents a Double from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroDouble private[express](value: Double) extends AvroValue(classOf[Double]) {
  override def asDouble(): Double = value
}

/**
 * Represents a Float from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroFloat private[express](value: Float) extends AvroValue(classOf[Float]) {
  override def asFloat(): Float = value
}

/**
 * Represents a String from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroString private[express](value: String) extends AvroValue(classOf[String]) {
  override def asString(): String = value
}

/**
 * Represents a byte array from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroByteArray private[express](value: Array[Byte])
    extends AvroValue(classOf[Array[Byte]]) {
  override def asBytes(): Array[Byte] = value
}

/**
 * Represents a List from an AvroRecord.  Elements are accessed using the apply method,
 * for example, `myList(0).asInt` gets the first element if it is a list of Ints.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroList private[express](value: List[AvroValue])
    extends AvroValue(classOf[List[AvroValue]]) {
  override def asList(): List[AvroValue] = value

  override def apply(index: Int): AvroValue = value(index)
}

/**
 * Represents a Map from an AvroRecord.  Values are accessed using the apply method,
 * for example, `myMap("key").asLong` gets the value corresponding to "key" if it is a Long.
 *
 * All keys are strings, since this represents an Avro map.
 *
 * @param value wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroMap private[express](value: Map[String, AvroValue])
    extends AvroValue(classOf[Map[String, AvroValue]]) {
  override def asMap(): Map[String, AvroValue] = value

  override def apply(key: String): AvroValue = value(key)
}

/**
 * Represents the string name of a Java Enum from an AvroRecord.
 *
 * @param name of the Enum wrapped by this AvroValue.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class AvroEnum(name: String)
    extends AvroValue(classOf[java.lang.Enum[_]]) {
  override def asEnumName(): String = name
}

/**
 * Represents an AvroRecord from a KijiCell.  This is KijiExpress's generic Avro API. Fields are
 * accessed using the apply method, for example, to access a field that is an Int:
 * `myRecord("myField").asInt()`.
 *
 * @param map from fields to values to put into the AvroRecord.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class AvroRecord private[express] (private[express] val map: Map[String, AvroValue])
    extends AvroValue(classOf[IndexedRecord]) {

  override def asRecord(): AvroRecord = this

  override def apply(field: String): AvroValue = {
    return map(field)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case AvroRecord(otherMap) => otherMap == map
      case _ => false
    }
  }

  override def hashCode(): Int = {
    map.hashCode
  }
}

/**
 * Companion object to AvroRecord containing factory methods for client use.
 */
object AvroRecord {
  /**
   * A factory method for creating an AvroRecord from a field mapping. The `fieldMap` argument can
   * be a Map or a sequence of (key, value) tuples.  If there are any duplicate keys, the last key
   * overrides all earlier keys.  If the ordering on fieldMap is undefined, then which duplicate
   * key's value makes it into the record is undefined.
   *
   * @param fieldMap is the underlying field mapping to use for the AvroRecord.
   * @return an AvroRecord with `fieldMap` as the field mapping.
   */
  def apply(fieldMap: Traversable[(String, Any)]): AvroRecord = {
    val recordFields = fieldMap.map {
      case (key, value) => (key, AvroUtil.scalaToGenericAvro(value))
    }.toMap[String, AvroValue]

    new AvroRecord(recordFields)
  }

  /**
   * A factory method for creating an AvroRecord from a field mapping.  This method allows the
   * notation `AvroRecord("key1" -> value1, "key2" -> value2)` to construct an AvroRecord.
   * If there are any duplicate keys, the last key overrides all earlier keys.
   *
   * @param fields is the underlying field mapping to use for the AvroRecord, in (key, value)
   *    tuples.
   * @return an AvroRecord with `fieldMap` as the field mapping.
   */
  def apply(fields: (String, Any)*): AvroRecord = {
    val recordFields = fields.map {
      case (key, value) => (key, AvroUtil.scalaToGenericAvro(value))
    }.toMap[String, AvroValue]

    new AvroRecord(recordFields)
  }

  /**
   * Extracts the underlying field mapping from an AvroRecord.
   *
   * @param avroRecord to extract the field mapping from.
   * @return the field mapping underlying `avroRecord`.
   */
  def unapply(avroRecord: AvroRecord): Option[Map[String, AvroValue]] = {
    Some(avroRecord.map)
  }
}

/**
 * Represents a Fixed (fixed-length byte array) from Avro.
 *
 * @param value wrapped by this AvroValue.
 */
final case class AvroFixed(fixedByteArray: Array[Byte])
    extends AvroValue(classOf[AvroFixed]) {
      override def asFixedBytes(): Array[Byte] = fixedByteArray
}
