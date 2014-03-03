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

import java.lang.reflect.Method

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.reflect.Manifest

import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TuplePacker
import com.twitter.scalding.TupleSetter
import com.twitter.scalding.TupleUnpacker
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.framework.serialization.KijiKryoExternalizer
import com.twitter.scalding.TupleConverter.ToMap

/**
 * Provides implementations of Scalding abstract classes to enable packing and unpacking Avro
 * specific and generic records.  Also provides implicit definitions to support implicitly using
 * these classes, where necessary.
 */
@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
trait AvroTupleConversions {
  /**
   * [[com.twitter.scalding.TuplePacker]] implementation provides instances of the
   * [[org.kiji.express.flow.util.AvroSpecificTupleConverter]] for converting fields
   * in a Scalding flow into specific Avro records of the parameterized type.  An instance of this
   * class must be in implicit scope, or passed in explicitly to
   * [[com.twitter.scalding.RichPipe.pack]].
   *
   * @tparam T type of compiled Avro class to be packed (created).
   * @param m [[scala.reflect.Manifest]] of T.  Provided implicitly by a built-in conversion.
   */
  private[express] class AvroSpecificTuplePacker[T <: SpecificRecord](implicit m: Manifest[T])
      extends TuplePacker[T] {
    override def newConverter(fields: Fields): TupleConverter[T] = {
      new AvroSpecificTupleConverter(fields, m)
    }
  }

  /**
   * Provides a [[com.twitter.scalding.TupleSetter]] for unpacking an Avro
   * [[org.apache.avro.generic.GenericRecord]] into a [[cascading.tuple.Tuple]].
   */
  private[express] class AvroGenericTupleUnpacker extends TupleUnpacker[GenericRecord] {
    override def newSetter(fields: Fields): TupleSetter[GenericRecord] = {
      new AvroGenericTupleSetter(fields)
    }
  }

  /**
   * Takes an Avro [[org.apache.avro.generic.GenericRecord]] and unpacks the specified fields
   * into a new [[cascading.tuple.Tuple]].
   *
   * @param fs the fields to be unpacked from the [[org.apache.avro.generic.GenericRecord]].
   */
  private[express] class AvroGenericTupleSetter(fs: Fields) extends TupleSetter[GenericRecord] {
    override def arity: Int = fs.size

    private val fields: List[String] = fs.iterator.asScala.map(_.toString).toList

    override def apply(arg: GenericRecord): Tuple = {
      new Tuple(fields.map(arg.get): _*)
    }
  }

  /**
   * Provides an [[org.kiji.express.flow.util.AvroTupleConversions.AvroSpecificTuplePacker]] to the
   * implicit scope.
   *
   * @tparam T Avro compiled [[org.apache.avro.specific.SpecificRecord]] class.
   * @param mf implicitly provided [[scala.reflect.Manifest]] of provided Avro type.
   * @return [[org.kiji.express.flow.util.AvroTupleConversions.AvroSpecificTuplePacker]] for given
   *         Avro specific record type
   */
  private[express] implicit def avroSpecificTuplePacker[T <: SpecificRecord]
      (implicit mf: Manifest[T]): AvroSpecificTuplePacker[T] = {
    new AvroSpecificTuplePacker[T]
  }

  /**
   * Provides an [[org.kiji.express.flow.util.AvroTupleConversions.AvroGenericTupleUnpacker]] to
   * implicit scope.
   *
   * @return an [[org.kiji.express.flow.util.AvroTupleConversions.AvroGenericTupleUnpacker]].
   */
  private[express] implicit def avroGenericTupleUnpacker: AvroGenericTupleUnpacker = {
    new AvroGenericTupleUnpacker
  }
}

/**
 * Converts [[cascading.tuple.TupleEntry]]s with the given fields into an Avro
 * [[org.apache.avro.specific.SpecificRecord]] instance of the parameterized type.  This
 * converter will fill in default values of the record if not specified by tuple fields.
 *
 * @tparam T Type of the target specific Avro record.
 * @param fs The fields to convert into an Avro record.  The field names must match the field
 *           names of the Avro record type.  There must be only one result field.
 * @param m [[scala.reflect.Manifest]] of the target type.  Implicitly provided.
 */
@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
private[express] case class AvroSpecificTupleConverter[T](fs: Fields, m: Manifest[T])
    extends TupleConverter[T] {

  import AvroTupleConversions._

  override def arity: Int = -1

  // Precompute as much of the reflection business as possible.  Method and Avro Schema objects do
  // not serialize, so any val containing one must be lazy (and transient if used during job setup).
  private val avroClass: Class[_] = m.erasure
  private val builderClass: Class[_] =
    avroClass.getDeclaredClasses.find(_.getSimpleName == "Builder").get
  lazy private val newBuilderMethod: Method = avroClass.getMethod("newBuilder")
  lazy private val buildMethod: Method = builderClass.getMethod("build")
  @transient lazy private val schema: Schema =
    avroClass.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]

  /** Mapping of canonical field name to value converter. */
  lazy private val converters: Map[String, Any => Any] = fieldConverters(schema)

  /** Mapping of method name to Method. */
  @transient lazy private val setters: Map[String, Method] =
    builderClass.getDeclaredMethods.map(m => m.getName -> m).toMap

  private def validate: Unit = {
    // Check that all scalding fields have a corresponding field in the schema
    val schemaFields: Set[String] = schema.getFields.asScala.map(_.name).map(snakeToCamel).toSet
    val schemaErrs = fs.asScala.collect {
      case field if !schemaFields(snakeToCamel(field.toString)) =>
        "Avro specific record " + avroClass + " does not contain field " + field + "."
    }

    // Check that all scalding fields have a corresponding setter in the specific record
    // This should catch the same issues as schemaErrs, but it's included to be thorough
    val setterErrs = fs.asScala.collect {
      case field if !setters.contains(fieldToSetter(field.toString)) =>
        "Avro specific record " + avroClass + " does not contain a setter for field " + field + "."
    }

    val errs = schemaErrs ++ setterErrs
    require(errs.isEmpty, errs.mkString("\n"))
  }
  validate

  override def apply(entry: TupleEntry): T = {
    val builder = newBuilderMethod.invoke(avroClass)
    ToMap(entry).foreach { case (field, value) =>
      setters(fieldToSetter(field))
        .invoke(builder, converters(snakeToCamel(field))(value).asInstanceOf[AnyRef])
    }
    buildMethod.invoke(builder).asInstanceOf[T]
  }
}

/**
 * Converts [[cascading.tuple.TupleEntry]]s into an Avro [[org.apache.avro.generic.GenericRecord]]
 * object with the provided schema.  This converter will fill in default values of the schema if
 * they are not specified through fields.
 *
 * @param schema of the target record
 */
@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
private[express] class AvroGenericTupleConverter(fs: Fields, schema: Schema)
    extends TupleConverter[GenericRecord] {

  import AvroTupleConversions._

  private val schemaExternalizer = KijiKryoExternalizer(schema)

  /** Mapping of canonical field name (CamelCased) to actual field name. */
  lazy private val fields: Map[String, String] = {
    val names = schemaExternalizer.get.getFields.asScala.map(_.name)
    names.map(snakeToCamel).zip(names).toMap
  }

  /** Mapping of canonical field name to value converter. */
  lazy private val converters: Map[String, Any => Any] = fieldConverters(schemaExternalizer.get)

  override def arity: Int = -1

  private def validate: Unit = {
    // Check that all scalding fields have a corresponding field in the schema
    val errs = fs.asScala.collect {
      case field if !fields.contains(snakeToCamel(field.toString)) =>
          "Avro generic record " + schemaExternalizer.get.getName + " does not contain field " +
              field + "."
    }

    require(errs.isEmpty, errs.mkString("\n"))
  }
  validate

  override def apply(entry: TupleEntry): GenericRecord = {
    val builder = new GenericRecordBuilder(schemaExternalizer.get)
    ToMap(entry).foreach { case (field, value) =>
      val canonical = snakeToCamel(field)
      builder.set(fields(canonical), converters(canonical)(value))
    }
    builder.build()
  }
}

/**
 * Provides utility functions for the avro tuple converter implementations.
 */
@ApiAudience.Private
@ApiStability.Stable
private[this] object AvroTupleConversions {
  /**
   * Attempts to translate an Avro field name (e.g. count, my_count, should_be_snake_case) to an
   * Avro record setter name (e.g., setCount, setMyCount, setShouldBeSnakeCase).
   *
   * @param field to translate to setter format.
   * @return setter of given field.
   */
  def fieldToSetter(field: String): String = {
    "set" + snakeToCamel(field).capitalize
  }

  /**
   * Attempts to translate a snake case name (e.g. count, my_count, should_be_snake_case) to a
   * camel case name (e.g., count, myCount, shouldBeSnakeCase).
   *
   * @param name to translate to camel case.
   * @return camel case version of supplied snake case name.
   */
  def snakeToCamel(name: String): String = {
    val components = name.split('_')
    (components(0) +: components.drop(1).map(_.capitalize)).mkString("")
  }

  /**
   * Creates a map of converter functions for a record's fields given a record schema. The
   * converter functions handle conversions between Scala types and the corresponding Avro generic
   * type.  The key of the map is the canonical form of the field name (camelCased).
   *
   * @param schema of record.
   * @return map of converter functions.
   */
  def fieldConverters(schema: Schema): Map[String, Any => Any] = {
    require(schema.getType == Schema.Type.RECORD)
    schema
      .getFields
      .asScala
      .map(field => snakeToCamel(field.name) -> AvroUtil.avroEncoder(field.schema))
      .toMap
  }
}
