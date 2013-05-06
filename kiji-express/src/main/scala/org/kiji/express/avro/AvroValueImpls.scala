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

package org.kiji.express.avro

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
 * Represents an Int from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroInt private[express](value: Int) extends AvroValue(classOf[Int]) {
  override def asInt(): Int = value
}

/**
 * Represents a Boolean from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroBoolean private[express](value: Boolean) extends AvroValue(classOf[Boolean]) {
  override def asBoolean(): Boolean = value
}

/**
 * Represents a Long from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroLong private[express](value: Long) extends AvroValue(classOf[Long]) {
  override def asLong(): Long = value
}

/**
 * Represents a Double from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroDouble private[express](value: Double) extends AvroValue(classOf[Double]) {
  override def asDouble(): Double = value
}

/**
 * Represents a Float from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroFloat private[express](value: Float) extends AvroValue(classOf[Float]) {
  override def asFloat(): Float = value
}

/**
 * Represents a String from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroString private[express](value: String) extends AvroValue(classOf[String]) {
  override def asString(): String = value
}

/**
 * Represents a byte array from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroByteArray private[express](value: Array[Byte])
    extends AvroValue(classOf[Array[Byte]]) {
  override def asBytes(): Array[Byte] = value
}

/**
 * Represents a List from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroList private[express](value: List[AvroValue])
    extends AvroValue(classOf[List[AvroValue]]) {
  override def asList(): List[AvroValue] = value

  override def apply(index: Int): AvroValue = value(index)
}

/**
 * Represents a Map from an AvroRecord.
 *
 * @param value wrapped by this AvroValue.
 */
case class AvroMap private[express](value: Map[String, AvroValue])
    extends AvroValue(classOf[Map[String, AvroValue]]) {
  override def asMap(): Map[String, AvroValue] = value

  override def apply(key: String): AvroValue = value(key)
}

/**
 * Represents the string name of a Java Enum from an AvroRecord.
 *
 * @param name of the Enum wrapped by this AvroValue.
 */
case class AvroEnum private[express](name: String)
    extends AvroValue(classOf[java.lang.Enum[_]]) {
  override def asEnumName(): String = name
}

/**
 * Represents an AvroRecord from a KijiCell.  This is KijiExpress's generic Avro API. Fields are
 * accessed using the apply method, for example, to access a field that is an Int:
 * `myRecord("myField").asInt()`
 *
 * @param record from the KijiCell.
 * @param schema of this AvroRecord.
 */
case class AvroRecord private[express](map: Map[String, AvroValue], schema: Schema)
    extends AvroValue(classOf[IndexedRecord]) {
  override def asRecord(): AvroRecord = this

  override def apply(field: String): AvroValue = {
    return map(field)
  }
}

/**
 * Companion object containing factory methods for AvroRecord.
 */
private[express] object AvroRecord {
  /**
   * Instantiates an AvroRecord from an IndexedRecord.
   *
   * @param record to instantiate from.
   * @return the equivalent AvroRecord.
   */
  private[express] def fromRecord(record: IndexedRecord): AvroRecord = {
    // Construct a map from field names in this record to the Scala-converted values.
    val schema = record.getSchema
    val recordMap =
        schema.getFields.asScala.map{
            field => (field.name, AvroUtil.wrapAvroTypes(record.get(field.pos)))
        }.toMap[String, AvroValue]
    // Use that map to construct an AvroRecord.
    new AvroRecord(recordMap, schema)
  }
}
