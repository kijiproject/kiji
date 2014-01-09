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

package org.kiji.express.flow.util

import java.nio.ByteBuffer

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

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
@ApiAudience.Framework
@ApiStability.Stable
object AvroUtil {

  /**
   * Attempts to convert complex Avro types to their Scala equivalents.  If no such conversion
   * exists, passes the value through.  In particular, four conversion are attempted:
   *
   * <ul>
   *   <li>Java [[java.util.Map]]s will be converted to Scala [[scala.collection.Map]].</li>
   *   <li>Java [[java.util.List]]s (including [[org.apache.avro.generic.GenericArray]]s) will be
   *       converted to a [[scala.collection.mutable.Buffer]]s.</li>
   *   <li>Avro [[org.apache.avro.generic.GenericFixed]] instances will be converted to Scala
   *       [[scala.Array[Byte]]]s.</li>
   *   <li>Avro [[org.apache.avro.generic.GenericEnumSymbol]]s will be converted to their
   *       [[java.lang.String]] equivalent.</li>
   * </ul>
   *
   * @param value to be converted to Scala equivalent
   * @return Scala equivalent, or original object if no such equivalent exists
   */
  private[express] def avroToScala(value: Any): Any = value match {
    case m: java.util.Map[_, _] => m.asScala.mapValues(avroToScala)
    case l: java.util.List[_] => l.asScala.map(avroToScala)
    case f: GenericFixed => f.bytes
    case e: GenericEnumSymbol => e.toString
    case other => other
  }

  /**
   * Provides an encoder function that will coerce a [[scala.collection.TraversableOnce]] to an
   * [[org.apache.avro.generic.GenericData.Array]].
   *
   * @param schema of the array
   * @return an encoding function for Avro array types.
   */
  private[express] def arrayEncoder(schema: Schema): Any => Any = {
    require(schema.getType == Schema.Type.ARRAY)
    val elementEncoder = avroEncoder(schema.getElementType)
    return {
      case tr: TraversableOnce[_] =>
        new GenericData.Array(schema, tr.map(elementEncoder).toList.asJava)
      case other => other
    }
  }

  /**
   * Provides an encoder function that will coerce a [[scala.collection.Map]] to a
   * [[java.util.Map]]. Avro does not provide a generic map type, so this is the closest type.
   */
  private[express] def mapEncoder(schema: Schema): Any => Any = {
    require(schema.getType == Schema.Type.MAP)
    val valueEncoder = avroEncoder(schema.getValueType)
    return {
      case map: Map[_, _] => map.mapValues(valueEncoder).asJava
      case other => other
    }
  }

  /**
   * Provides an encoder function that will coerce values to an
   * [[org.apache.avro.generic.GenericData.EnumSymbol]] if possible.
   *
   * @param schema of enum type
   * @return an encoder function for enum values.
   */
  private[express] def enumEncoder(schema: Schema): Any => Any = {
    require(schema.getType == Schema.Type.ENUM)
    val genericData = new GenericData()

    return {
      case e: Enum[_] =>
        // Perhaps useful for the case where a user defines their own enum with the same members
        // as defined in the schema.
        genericData.createEnum(e.name, schema)
      case s: String => genericData.createEnum(s, schema)
      case other => other
    }
  }

  /**
   * Provides an encoder function that will coerce values into an Avro compatible bytes format.
   *
   * @param schema of bytes type
   * @return an encoder function for bytes values
   */
  private[express] def bytesEncoder(schema: Schema): Any => Any = {
    require(schema.getType == Schema.Type.BYTES)

    return {
      case bs: Array[Byte] => ByteBuffer.wrap(bs)
      case other => other
    }
  }

  /**
   * Provides an encoder function that will coerce values into an Avro generic fixed.
   *
   * @param schema of fixed type.
   * @return an encoder function for fixed values.
   */
  private[express] def fixedEncoder(schema: Schema): Any => Any = {
    require(schema.getType == Schema.Type.FIXED)

    def getBytes(bb: ByteBuffer): Array[Byte] = {
      val backing = bb.array()
      val remaining = bb.remaining()
      if (remaining == backing.length) { // no need to copy
        backing
      } else {
        val copy = Array.ofDim[Byte](remaining)
        bb.get(copy)
        copy
      }
    }

    return {
      case bs: Array[Byte] => new Fixed(schema, bs)
      case bb: ByteBuffer if bb.hasArray => new Fixed(schema, getBytes(bb))
      case other => other
    }
  }

  /**
   * Creates an encoder function for a given schema.  For most cases no encoding needs to be done,
   * so the encoder is the identity function.  For the collection types that have generic
   * implementations we can convert a scala version of the type to the generic avro version.  We
   * don't try to catch type mismatches here; instead Schema will check if the value is compatible
   * with the given schema.
   *
   * @param schema of the values that the encoder function will take.
   * @return an encoder function that will make a best effort to encode values to a type
   *    compatible with the given schema.
   */
  private[express] def avroEncoder(schema: Schema): Any => Any = {
    schema.getType match {
      case Schema.Type.ARRAY => arrayEncoder(schema)
      case Schema.Type.MAP   => mapEncoder(schema)
      case Schema.Type.ENUM  => enumEncoder(schema)
      case Schema.Type.BYTES => bytesEncoder(schema)
      case Schema.Type.FIXED => fixedEncoder(schema)
      case _ => identity
    }
  }
}
