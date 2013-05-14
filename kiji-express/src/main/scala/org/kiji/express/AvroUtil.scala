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

import java.io.InvalidClassException

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificFixed
import org.apache.avro.specific.SpecificRecord

import org.kiji.annotations.ApiAudience

/**
 * A module with functions that can convert Java values (including Java API Avro values)
 * read from a Kiji table to Scala values (including our Express-specific generic API Avro values)
 * for use in KijiExpress, and vice versa for writing back to a Kiji table.
 *
 * When reading from Kiji, using the specific API, use the method `decodeSpecificFromJava`.
 * When reading from Kiji, using the generic API, use the method `decodeGenericFromJava`.
 * When writing to Kiji, use the method `encodeToJava`.
 */
 @ApiAudience.Private
private[express] object AvroUtil {
  /**
   * Convert Java types (that came from Avro-deserialized Kiji columns) into corresponding Scala
   * types, using the generic API, for usage within KijiExpress.
   *
   * Primitives are converted to their corresponding Scala primitives.  Records are converted
   * to AvroRecords via the wrapGenericAvro helper function; primitives inside AvroRecords are
   * converted to their corresponding AvroValues using the `wrapGenericAvro` function.
   *
   * @param columnValue is the value originally read from the Kiji column.
   * @return the corresponding value converted to a Scala type using the generic AvroValue API if
   *     necessary.
   */
  private[express] def decodeGenericFromJava(x: Any): Any = {
    x match {
      case record: IndexedRecord => wrapGenericAvro(record)
      case nonrecord => javaTypesToScala(nonrecord)
    }
  }

  /**
   * Convert Java types (that came from Avro-deserialized Kiji columns) into corresponding Scala
   * types, using the specific API, for usage within KijiExpress.
   *
   * @param columnValue is the value originally read from the Kiji column.
   * @return the corresponding value converted to a Scala type.
   */
  private[express] def decodeSpecificFromJava(x: Any): Any = {
    x match {
      // specific record already on the classpath
      case record: SpecificRecord => record
      // generic record
      case generic: GenericRecord => throw new InvalidClassException("Cannot read generic record"
          + "%s using the specific API.".format(generic))
      case nonrecord => javaTypesToScala(nonrecord)
    }
  }

  /**
   * Convert Scala types from Express (including generic AvroValue types) into corresponding
   * Java types, to write back to Kiji.
   *
   * @param x is the object to change into a Kiji-writable type.
   * @param schema that `x` will be written with.
   * @return the Java object that can be written to Kiji.
   */
  private[express] def encodeToJava(x: Any, schema: Schema): Any = {
    x match {
      case genericValue: AvroValue => unwrapGenericAvro(genericValue, schema)
      case nongeneric => scalaTypesToJava(nongeneric, schema)
    }
  }

  /**
   * Wraps Java objects that correspond to Avro values into their appropriate AvroValue class.
   *
   * @param x is the object to wrap.
   * @return the wrapped object as an AvroValue.
   */
  private[express] def wrapGenericAvro(x: Any): AvroValue = {
    x match {
      case record: IndexedRecord => {
        // Construct a map from field names in this record to the Scala-converted values.
        val schema = record.getSchema
        val recordMap =
            schema.getFields.asScala.map {
                field => (field.name, wrapGenericAvro(record.get(field.pos)))
            }.toMap[String, AvroValue]
        // Use that map to construct an AvroRecord.
        new AvroRecord(recordMap)
      }
      // Recursively convert lists or maps.
      case l: java.util.List[_] => new AvroList(l.asScala.toList.map(wrapGenericAvro(_)))
      case m: java.util.Map[_, _] =>
          new AvroMap(m.asScala.toMap.map { case (key: CharSequence, value) =>
              (key.toString, wrapGenericAvro(value)) }
          )
      case enumSymbol: GenericData.EnumSymbol => {
        new AvroEnum(enumSymbol.toString)
      }
      case javatype => { javaTypesToScala(javatype) match {
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
        case l: List[_] => new AvroList(l.map(wrapGenericAvro(_)))
        case m: Map[_, _] =>
            new AvroMap(m.map { case (key: CharSequence, value) =>
                (key.toString, wrapGenericAvro(value)) }
            )
        case e: java.lang.Enum[_] => new AvroEnum(e.name)
        case _ => throw new InvalidClassException(
            "Object %s of type %s cannot be converted to an AvroValue."
                .format(x, x.getClass))
      }}
    }
  }

  /**
   * Convert Java types (that came from Avro-deserialized Kiji columns) into corresponding Scala
   * types for usage within KijiExpress.
   *
   * @param javaValue is the value to convert.
   * @return the corresponding value converted to a Scala type.
   */
  private[express] def javaTypesToScala(javaValue: Any): Any = {
    javaValue match {
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
            .map { elem => javaTypesToScala(elem) }
      }
      // map (avro maps always have keys of type CharSequence)
      // TODO CHOP-70 revisit conversion of maps between java and scala
      case m: java.util.Map[_, _] => {
        m.asScala
            .toMap
            .map { case (key: CharSequence, value) =>
              (key.toString, javaTypesToScala(value))
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
      // any other type we don't understand
      case _ => {
        val errorMsgFormat = "Read an unrecognized Java object %s with type %s from Kiji that " +
            "could not be converted to a scala type for use with KijiExpress."
        throw new InvalidClassException(errorMsgFormat.format(javaValue, javaValue.getClass))
      }
    }
  }

  /**
   * Unwraps AvroValues into their corresponding Java Objects for writing to a Kiji table.
   *
   * @param avroValue to unwrap.
   * @param columnSchema of the avroValue to unwrap.
   * @return the corresponding Java Object.
   */
  private[express] def unwrapGenericAvro(
      avroValue: AvroValue,
      columnSchema: Schema): java.lang.Object = {
    avroValue match {
      // AvroValues
      case AvroInt(i) => scalaTypesToJava(i, columnSchema)
      case AvroBoolean(b) => scalaTypesToJava(b, columnSchema)
      case AvroLong(l) => scalaTypesToJava(l, columnSchema)
      case AvroFloat(f) => scalaTypesToJava(f, columnSchema)
      case AvroDouble(d) => scalaTypesToJava(d, columnSchema)
      case AvroString(s) => scalaTypesToJava(s, columnSchema)
      case AvroByteArray(bb) => scalaTypesToJava(bb, columnSchema)
      case AvroList(l: List[_]) => {
        // scalastyle:off null
        require(null != columnSchema, "Tried to convert a List[Any], but the writer schema was" +
            " null.")
        // scalastyle:on null
        l.map { elem => unwrapGenericAvro(elem, columnSchema.getElementType) }
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
          val convertedValue = unwrapGenericAvro(value, columnSchema.getValueType)
          (key, convertedValue)
        }
        new java.util.TreeMap[java.lang.Object, java.lang.Object](convertedMap.asJava)
      }
      // enum
      case AvroEnum(name: String) => {
        new GenericData.EnumSymbol(columnSchema, name)
      }
      // avro record or object
      case AvroRecord(map: Map[_, _]) => {
        val record = new GenericData.Record(columnSchema)
        for ((fieldname, value) <- map) {
          record.put(fieldname, unwrapGenericAvro(value, columnSchema.getField(fieldname).schema))
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
  private[express] def scalaTypesToJava(
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
            + "was null.")
        // scalastyle:on null
        if (columnSchema.getType == Schema.Type.BYTES) {
          java.nio.ByteBuffer.wrap(bb)
        } else if (columnSchema.getType == Schema.Type.FIXED) {
          new GenericData.Fixed(columnSchema, bb)
        } else {
          throw new SchemaMismatchException("Writing an array of bytes to a column that "
              + " expects " + columnSchema.getType.getName)
        }
      }
      case l: List[_] => {
        // scalastyle:off null
        require(null != columnSchema, "Tried to convert a List[Any], but the writer schema was" +
            " null.")
        // scalastyle:on null
        l.map { elem => scalaTypesToJava(elem, columnSchema.getElementType) }
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
          val convertedValue = scalaTypesToJava(value, columnSchema.getValueType)
          (key, convertedValue)
        }
        new java.util.TreeMap[java.lang.Object, java.lang.Object](convertedMap.asJava)
      }
      // enum
      case e: java.lang.Enum[_] => e
      // Avro records
      case r: IndexedRecord => r
      // AvroValue
      case avroValue: AvroValue => unwrapGenericAvro(avroValue, columnSchema)

      // If none of the cases matched:
      case _ => throw new InvalidClassException("Trying to write an unrecognized Scala type"
          + " that cannot be converted to a Java type for writing to Kiji: "
          + columnValue.asInstanceOf[AnyRef].getClass.toString)
    }
  }
}
