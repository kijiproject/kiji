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

import java.io.InvalidClassException

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificFixed
import org.apache.avro.specific.SpecificRecord

/**
 * Object that defines utility functions for Avro-Scala conversions.
 *
 * There are four functions in this module: convertJavaTypes, convertScalaTypes, wrapAvroTypes,
 * and unwrapAvroTypes.
 *
 * To convert from:               use:
 *   Scala to Java                   convertScalaTypes
 *   Java to Scala                   convertJavaTypes
 *   Scala to AvroValues             wrapAvroTypes
 *   AvroValues to Java              unwrapAvroTypes
 *
 * In other words:
 * when reading from Kiji, use: convertJavaTypes (Java to Scala)
 * when writing to Kiji, if using the generic API, use: unwrapAvroTypes (AvroValues to Java)
 * when writing to Kiji, if using the specific API, use: convertScalaTypes (Scala to Java)
 *
 * wrapAvroTypes is only called from convertJavaTypes, when necessary for converting a generic
 * Avro record.
 */
object AvroUtil {
  /**
   * Wraps Java objects that correspond to Avro values into their appropriate AvroValue class.
   *
   * @param x is the object to wrap.
   * @return the wrapped object as an AvroValue.
   */
  private[express] def wrapAvroTypes(x: Any): AvroValue = {
    val converted = convertJavaTypes(x)
    converted match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      case i: Int => new AvroInt(i)
      case b: Boolean => new AvroBoolean(b)
      case l: Long => new AvroLong(l)
      case f: Float => new AvroFloat(f)
      case d: Double => new AvroDouble(d)
      case b: Array[Byte] => new AvroByteArray(b)
      case s: String => new AvroString(s)
      case l: List[_] => new AvroList(l.map(wrapAvroTypes(_)))
      case m: Map[_, _] =>
          new AvroMap(m.map { case (key: CharSequence, value) =>
              (key.toString, wrapAvroTypes(value)) }
          )
      case e: java.lang.Enum[_] => new AvroEnum(e.name)
      case record: GenericRecord => AvroRecord.fromRecord(record)
      case _ => throw new InvalidClassException(
          "Object %s of type %s cannot be converted to an AvroValue."
              .format(converted, converted.getClass))
    }
  }

  /**
   * Convert Java types (that came from Avro-deserialized Kiji columns) into corresponding Scala
   * types for usage within KijiExpress.
   *
   * Primitives are converted to their corresponding Scala primitives.  Records are converted
   * to AvroRecords via the wrapAvroTypes helper function; primitives inside AvroRecords are
   * converted to their corresponding AvroValues using the wrapAvroTypes function.
   *
   * @param columnValue is the value of the Kiji column.
   * @return the corresponding value converted to a Scala type.
   */
  private[express] def convertJavaTypes(columnValue: Any): Any = {
    columnValue match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      case i: java.lang.Integer => i
      case b: java.lang.Boolean => b
      case l: java.lang.Long => l
      case f: java.lang.Float => f
      case d: java.lang.Double => d
      // bytes
      case bb: java.nio.ByteBuffer => bb.array()
      // string
      case s: java.lang.CharSequence => s.toString
      // array
      case l: java.util.List[_] => {
        l.asScala
            .toList
            .map { elem => convertJavaTypes(elem) }
      }
      // map (avro maps always have keys of type CharSequence)
      // TODO CHOP-70 revisit conversion of maps between java and scala
      case m: java.util.Map[_, _] => {
        m.asScala
            .toMap
            .map { case (key: CharSequence, value) =>
              (key.toString, convertJavaTypes(value))
            }
      }
      // fixed
      case f: SpecificFixed => f.bytes().array
      // scalastyle:off null
      // null field
      case n: java.lang.Void => null
      // scalastyle:on null
      // enum
      case e: java.lang.Enum[_] => e
      // specific record already on the classpath
      case s: SpecificRecord => s
      // generic avro record or object
      // This drops into the wrapAvroTypes function via AvroRecord.fromRecord.
      // From here on, everything is in the generic API.
      case record: GenericRecord => AvroRecord.fromRecord(record)
      // any other type we don't understand
      case _ => throw new InvalidClassException("Read an unrecognized Java type from Kiji "
          + "that could not be converted to a Scala type for use with KijiExpress: "
          + columnValue.asInstanceOf[AnyRef].getClass.toString)
    }
  }

  /**
   * Unwraps AvroValues into their corresponding Java Objects for writing to a Kiji table.
   *
   * @param avroValue to unwrap.
   * @param columnSchema of the avroValue to unwrap.
   * @return the corresponding Java Object.
   */
  private[express] def unwrapAvroTypes(
      avroValue: AvroValue,
      columnSchema: Schema): java.lang.Object = {
    avroValue match {
      // AvroValues
      case AvroInt(i) => convertScalaTypes(i, columnSchema)
      case AvroBoolean(b) => convertScalaTypes(b, columnSchema)
      case AvroLong(l) => convertScalaTypes(l, columnSchema)
      case AvroFloat(f) => convertScalaTypes(f, columnSchema)
      case AvroDouble(d) => convertScalaTypes(d, columnSchema)
      case AvroString(s) => convertScalaTypes(s, columnSchema)
      case AvroByteArray(bb) => convertScalaTypes(bb, columnSchema)
      case AvroList(l: List[_]) => {
        // scalastyle:off null
        require(null != columnSchema, "Tried to convert a List[Any], but the writer schema was" +
            " null.")
        // scalastyle:on null
        l.map { elem => unwrapAvroTypes(elem, columnSchema.getElementType) }
            .asJava
      }
      // map
      // TODO CHOP-70 revisit conversion of maps between java and scala
      case AvroMap(m: Map[_, _]) => {
        // scalastyle:off null
        require(null != columnSchema, "Tried to convert a Map[String, Any], but the writer schema"
          + "was null.")
        // scalastyle:on null
        val convertedMap = m.map { case (key: String, value) =>
          val convertedValue = unwrapAvroTypes(value, columnSchema.getValueType)
          (key, convertedValue)
        }
        new java.util.TreeMap[java.lang.Object, java.lang.Object](convertedMap.asJava)
      }
      // enum
      case AvroEnum(name: String) => {
        new GenericData.EnumSymbol(columnSchema, name)
      }
      // avro record or object
      case AvroRecord(map: Map[_, _], schema: Schema) => {
        val record = new GenericData.Record(schema)
        for ((fieldname, value) <- map) {
          record.put(fieldname, unwrapAvroTypes(value, schema.getField(fieldname).schema))
        }
        record
      }

      // If none of the cases matched:
      case _ => throw new InvalidClassException("Trying to write an unrecognized Scala type"
          + " that cannot be converted to a Java type for writing to Kiji: "
          + avroValue.asInstanceOf[AnyRef].getClass.toString)
    }
  }

  /**
   * Converts Scala types back to Java types to write to a Kiji table.
   *
   * @param columnValue is the value written to this column.
   * @return the converted Java type.
   */
  private[express] def convertScalaTypes(
      columnValue: Any,
      columnSchema: Schema): java.lang.Object = {
    columnValue match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      case i: Int => i.asInstanceOf[java.lang.Integer]
      case b: Boolean => b.asInstanceOf[java.lang.Boolean]
      case l: Long => l.asInstanceOf[java.lang.Long]
      case f: Float => f.asInstanceOf[java.lang.Float]
      case d: Double => d.asInstanceOf[java.lang.Double]
      case s: String => s
      case bb: Array[Byte] => {
        // scalastyle:off null
        require(null != columnSchema, "Tried to convert an Array[Byte], but the writer schema"
        // scalastyle:on null
            + "was null.")
        if (columnSchema.getType == Schema.Type.BYTES) {
          java.nio.ByteBuffer.wrap(bb)
        } else if (columnSchema.getType == Schema.Type.FIXED) {
          new Fixed(columnSchema, bb)
        } else {
          throw new SchemaMismatchException("Writing an array of bytes to a column that "
              + " expects " + columnSchema.getType.getName)
        }
      }
      // this is an avro array
      case l: List[_] => {
        // scalastyle:off null
        require(null != columnSchema, "Tried to convert a List[Any], but the writer schema was" +
            " null.")
        // scalastyle:on null
        l.map { elem => convertScalaTypes(elem, columnSchema.getElementType) }
            .asJava
      }
      // map
      // TODO CHOP-70 revisit conversion of maps between java and scala
      case m: Map[_, _] => {
        // scalastyle:off null
        require(null != columnSchema, "Tried to convert a Map[String, Any], but the writer schema" +
          "was null.")
        // scalastyle:on null
        val convertedMap = m.map { case (key: String, value) =>
          val convertedValue = convertScalaTypes(value, columnSchema.getValueType)
          (key, convertedValue)
        }
        new java.util.TreeMap[java.lang.Object, java.lang.Object](convertedMap.asJava)
      }
      // enum
      case e: java.lang.Enum[_] => e
      // Avro records
      case r: IndexedRecord => r
      // AvroValue
      case avroValue: AvroValue => unwrapAvroTypes(avroValue, columnSchema)

      // If none of the cases matched:
      case _ => throw new InvalidClassException("Trying to write an unrecognized Scala type"
          + " that cannot be converted to a Java type for writing to Kiji: "
          + columnValue.asInstanceOf[AnyRef].getClass.toString)
    }
  }
}
