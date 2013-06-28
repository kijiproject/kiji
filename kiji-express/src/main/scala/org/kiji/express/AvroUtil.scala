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
import java.util.UUID

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
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
 *
 * When decoding using the generic API, primitives are converted to Scala primitives, and records
 * are converted to AvroValues. The contents of a record are also AvroValues, even if they
 * represent primitives.  These primitives can be accessed with `asInt`, `asLong`, etc methods.
 * For example: `myRecord("fieldname").asInt` gets an integer field with name `fieldname` from
 * `myRecord`.
 *
 * When decoding using the specific API, primitives are converted to Scala primitives, and records
 * are converted to their corresponding Java classes.
 *
 * Certain AvroValues are used in both the specific and the generic API:
 * <ul>
 *   <li>AvroEnum, which always wraps an enum from Avro.  This is because there is not an easy
 *       Scala equivalent of a Java enum.</li>
 *   <li>AvroFixed, which always wraps a fixed-length byte array from Avro.  This is to distinguish
 *       between a regular byte array, which gets converted to an Array[Byte], and a fixed-length
 *       byte array.</li>
 * <ul>
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
   * @param x is the value originally read from the Kiji column.
   * @return the corresponding value converted to a Scala type using the generic AvroValue API if
   *     necessary.
   */
  private[express] def decodeGenericFromJava(x: Any): Any = {
    x match {
      case record: IndexedRecord => wrapGenericAvro(record)
      case enumSymbol: GenericData.EnumSymbol => {
        new AvroEnum(enumSymbol.toString)
      }
      case nonrecord => javaToScala(nonrecord)
    }
  }

  /**
   * Convert Java types (that came from Avro-deserialized Kiji columns) into corresponding Scala
   * types, using the specific API, for usage within KijiExpress.
   *
   * @param x is the value originally read from the Kiji column.
   * @return the corresponding value converted to a Scala type.
   */
  private[express] def decodeSpecificFromJava(x: Any): Any = {
    x match {
      // specific record already on the classpath
      case record: SpecificRecord => record
      // generic record
      case generic: GenericRecord => throw new InvalidClassException("Cannot read generic record"
          + "%s using the specific API.".format(generic))
      case enumSymbol: GenericData.EnumSymbol => {
        new AvroEnum(enumSymbol.toString)
      }
      case nonrecord => javaToScala(nonrecord)
    }
  }

  /**
   * Convert Scala types from Express (including generic AvroValue types) into corresponding
   * Java types, to write back to Kiji.
   *
   * @param x is the object to change into a Kiji-writable type.
   * @return the Java object that can be written to Kiji.
   */
  private[express] def encodeToJava(x: Any): Any = {
    x match {
      case genericValue: AvroValue => unwrapGenericAvro(genericValue)
      case nongeneric => scalaToJava(nongeneric)
    }
  }

  /**
   * Converts a Scala object into its corresponding AvroValue class.
   *
   * @param x is the Scala value to convert.
   * @return The corresponding AvroValue for x.
   */
  private[express] def scalaToGenericAvro(x: Any): AvroValue = {
    x match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      // AvroEnums and AvroRecords are already generic avro.
      case avro: AvroValue => avro
      case list: List[_] => new AvroList(list.map(scalaToGenericAvro(_)))
      case map: Map[_, _] => {
        new AvroMap(map.asInstanceOf[Map[String, _]].mapValues(scalaToGenericAvro(_)))
      }
      case primitive => wrapGenericAvro(scalaToJava(primitive))
    }
  }

  /**
   * Wraps Java objects that correspond to Avro values into their appropriate AvroValue class.
   *
   * If `x` is already an AvroValue, this returns `x` with no change.
   *
   * @param x is the object to wrap.
   * @return the wrapped object as an AvroValue.
   */
  private[express] def wrapGenericAvro(x: Any): AvroValue = {
    x match {
      case v: AvroValue => v
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
      case enum: java.lang.Enum[_] => new AvroEnum(enum.name)
      // EnumSymbol from an Avro record.
      case enumSymbol: GenericData.EnumSymbol => {
        new AvroEnum(enumSymbol.toString)
      }
      // primitives
      case javatype => { javaToScala(javatype) match {
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
  private[express] def javaToScala(javaValue: Any): Any = {
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
            .map { elem => decodeGenericFromJava(elem) }
      }
      // map (avro maps always have keys of type CharSequence)
      // TODO(EXP-51): revisit conversion of maps between java and scala
      case m: java.util.Map[_, _] => {
        m.asScala
            .toMap
            .map { case (key: CharSequence, value) =>
              (key.toString, decodeGenericFromJava(value))
            }
      }
      // fixed
      case f: SpecificFixed => new AvroFixed(f.bytes().array)
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
   * @return the corresponding Java Object.
   */
  private[express] def unwrapGenericAvro(
      avroValue: AvroValue): java.lang.Object = {
    /**
     * Inner function that unwraps AvroValues and returns the Object along with a generic version
     * of its Avro Schema.
     *
     * @param avroValue to unwrap.
     * @return the corresponding Java Object along with its Avro Schema.
     */
    def unwrapGenericWithSchema(avroValue: AvroValue): (java.lang.Object, Schema) = {
      avroValue match {
        // If it's null, the schema is also null.
        // scalastyle:off null
        case null => (null, Schema.create(Schema.Type.NULL))
        // scalastyle:on null
        // AvroValues
        case AvroInt(i) => (scalaToJava(i), Schema.create(Schema.Type.INT))
        case AvroBoolean(b) => (scalaToJava(b), Schema.create(Schema.Type.BOOLEAN))
        case AvroLong(l) => (scalaToJava(l), Schema.create(Schema.Type.LONG))
        case AvroFloat(f) => (scalaToJava(f), Schema.create(Schema.Type.FLOAT))
        case AvroDouble(d) => (scalaToJava(d), Schema.create(Schema.Type.DOUBLE))
        case AvroString(s) => (scalaToJava(s), Schema.create(Schema.Type.STRING))
        case AvroByteArray(bb) => (scalaToJava(bb), Schema.create(Schema.Type.BYTES))
        case AvroFixed(fixed) => {
          val schema = Schema.createFixed(
              getRandomSchemaName(),
              "A Fixed Schema generated by KijiExpress with no namespace.",
              // scalastyle:off null
              null,
              // scalastyle:on null
              fixed.size)
          (new GenericData.Fixed(schema, fixed), schema)
        }
        case AvroList(l: List[_]) => {
          val (values, schemas) = l.map { elem => unwrapGenericWithSchema(elem) }.unzip
          val elementSchema = {
            if (schemas.isEmpty) {
              // scalastyle:off null
              null
              // scalastyle:on null
            } else {
              // Require that if schemas is non-empty, all of them must be the same type.
              require(schemas.forall(_.getType == schemas.head.getType))
              schemas.head
            }
          }
          (values.asJava, Schema.createArray(elementSchema))
        }
        // map
        // TODO(EXP-51): revisit conversion of maps between java and scala
        case AvroMap(m: Map[_, _]) => {
          val convertedMap = m.map { case (key: String, value) => (key, unwrapGenericAvro(value)) }
          val javaMap =
              new java.util.TreeMap[java.lang.Object, java.lang.Object](convertedMap.asJava)
          val (_, schemas) = m.values.map(value => unwrapGenericWithSchema(value)).unzip
          val elementSchema = {
            if (schemas.isEmpty) {
              // scalastyle:off null
              null
              // scalastyle:on null
            } else {
              // Require that if schemas is non-empty, all of them must be the same.
              require(schemas.forall(_ == schemas.head))
              schemas.head
            }
          }
          (javaMap, Schema.createMap(elementSchema))
        }
        // enum
        case AvroEnum(name: String) => {
          val enumSchema = Schema.createEnum(
              getRandomSchemaName(),
              "A schema constructed by KijiExpress, with no namespace.",
              // scalastyle:off null
              null, // No namespace.
              // scalastyle:on null
              List(name).asJava)
          (new GenericData.EnumSymbol(enumSchema, name), enumSchema)
        }
        // avro record or object
        case AvroRecord(map: Map[_, _]) => {
          val fields: List[Schema.Field] = map.map {
            case (fieldname, value) => {
              new Schema.Field(
                  fieldname,
                  unwrapGenericWithSchema(value)._2,
                  "A default field constructed by KijiExpress.",
                  // scalastyle:off null
                  null) // No namespace.
                  // scalastyle:on null
            }
          }.toList
          // scalastyle:off null
          val schema = Schema.createRecord(getRandomSchemaName(),
              "A Record schema generated by KijiExpress with no namespace.", null, false)
          // scalastyle:on null
          schema.setFields(fields.asJava)
          val record = new GenericData.Record(schema)
          for ((fieldname, value) <- map) {
            record.put(fieldname, unwrapGenericAvro(value))
          }
          (record, schema)
        }
        case _ => throw new InvalidClassException("Trying to write an unrecognized Scala type"
            + " that cannot be converted to a Java type for writing to Kiji: "
            + avroValue.getClass)
      }
    }

    val (javaValue, schema) = unwrapGenericWithSchema(avroValue)
    return javaValue
  }

  /**
   * Generates a name for a schema consisting of the prefix `schema-` followed by a randomly
   * generated UUID, in which the `-` character has been replaced with `_`.
   *
   * @return the generated schema name.
   */
  private def getRandomSchemaName(): String = {
    "schema_" + UUID.randomUUID().toString().replaceAllLiterally("-", "_")
  }

  /**
   * Converts Scala types back to Java types to write to a Kiji table.
   *
   * @param columnValue is the value written to this column.
   * @return the converted Java type.
   */
  private[express] def scalaToJava(
      columnValue: Any): java.lang.Object = {
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
      case bytes: Array[Byte] => java.nio.ByteBuffer.wrap(bytes)
      case l: List[_] => {
        l.map { elem => scalaToJava(elem) }
            .asJava
      }
      // map
      // TODO(EXP-51): revisit conversion of maps between java and scala
      case m: Map[_, _] => {
        val convertedMap = m.map { case (key: String, value) => (key, scalaToJava(value)) }
        new java.util.TreeMap[java.lang.Object, java.lang.Object](convertedMap.asJava)
      }
      // enum
      case e: java.lang.Enum[_] => e
      // Avro records
      case r: IndexedRecord => r
      // AvroValue
      case avroValue: AvroValue => unwrapGenericAvro(avroValue)
      case _ => throw new InvalidClassException("Trying to write an unrecognized Scala type"
          + " that cannot be converted to a Java type for writing to Kiji: "
          + columnValue.asInstanceOf[AnyRef].getClass.toString)
    }
  }
}
