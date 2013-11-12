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

package org.kiji.express.util

import java.lang.reflect.Method

import scala.collection.JavaConversions.asScalaIterator
import scala.reflect.Manifest

import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.twitter.scalding.TupleConversions
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
import org.kiji.express.flow.framework.serialization.KijiLocker

/**
 * Provides implementations of Scalding abstract classes to enable packing and unpacking Avro
 * specific and generic records.  Also provides implicit definitions to support implicitly using
 * these classes, where necessary.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
trait AvroTupleConversions {
  /**
   * [[com.twitter.scalding.TuplePacker]] implementation provides instances of the
   * [[org.kiji.express.util.AvroSpecificTupleConverter]] for converting fields
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

    private val fields: List[String] = fs.iterator.map(_.toString).toList

    override def apply(arg: GenericRecord): Tuple = {
      new Tuple(fields.map(arg.get): _*)
    }
  }

  /**
   * Provides an [[org.kiji.express.util.AvroTupleConversions.AvroSpecificTuplePacker]] to the
   * implicit scope.
   *
   * @tparam T Avro compiled [[org.apache.avro.specific.SpecificRecord]] class.
   * @param mf implicitly provided [[scala.reflect.Manifest]] of provided Avro type.
   * @return [[org.kiji.express.util.AvroTupleConversions.AvroSpecificTuplePacker]] for given Avro
   *         specific record type
   */
  private[express] implicit def avroSpecificTuplePacker[T <: SpecificRecord]
      (implicit mf: Manifest[T]): AvroSpecificTuplePacker[T] = {
    new AvroSpecificTuplePacker[T]
  }

  /**
   * Provides an [[org.kiji.express.util.AvroTupleConversions.AvroGenericTupleUnpacker]] to implicit
   * scope.
   *
   * @return an [[org.kiji.express.util.AvroTupleConversions.AvroGenericTupleUnpacker]] instance.
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
@ApiStability.Experimental
@Inheritance.Sealed
private[express] case class AvroSpecificTupleConverter[T](fs: Fields, m: Manifest[T])
    extends TupleConverter[T] {

  override def arity: Int = -1

  /**
   * Attempts to translate an avro field name (e.g. count, my_count, should_be_snake_case) to an
   * Avro record setter name (e.g., setCount, setMyCount, setShouldBeSnakeCase).
   * @param field to translate to setter format.
   * @return setter of given field.
   */
  private def fieldToSetter(field: String): String = {
    field.split('_').map(_.capitalize).mkString("set", "", "")
  }

  // Precompute as much of the reflection business as possible.  Method objects do not serialize,
  // so any val containing a method in it must be lazy.
  private val avroClass: Class[_] = m.erasure
  private val builderClass: Class[_] =
      avroClass.getDeclaredClasses.find(_.getSimpleName == "Builder").get
  lazy private val newBuilderMethod: Method = avroClass.getMethod("newBuilder")
  lazy private val buildMethod: Method = builderClass.getMethod("build")

  /**
   * Map of field name to setter method.
   */
  lazy private val fieldSetters: Map[String, Method] = {
    val fields: List[String] = fs.iterator.map(_.toString).toList
    val setters: Map[String, Method] = builderClass
        .getDeclaredMethods
        .map { m => (m.getName, m) }
        .toMap

    fields
        .zip(fields.map(fieldToSetter))
        .toMap
        .mapValues(setters)
  }

  override def apply(entry: TupleEntry): T = {
    val builder = newBuilderMethod.invoke(avroClass)
    fieldSetters.foreach { case (field, setter) => setter.invoke(builder, entry.getObject(field)) }
    buildMethod.invoke(builder).asInstanceOf[T]
  }
}

/**
 * Converts [[cascading.tuple.TupleEntry]]s into an Avro [[org.apache.avro.generic.GenericRecord]]
 * object with the provided schema.  This converter will fill in default values of the schema if
 * they are not specified through fields.
 *
 * @param schemaLocker wrapping the schema of the target record
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[express] class AvroGenericTupleConverter(schemaLocker: KijiLocker[Schema])
    extends TupleConverter[GenericRecord] with TupleConversions {

  override def arity: Int = -1

  override def apply(entry: TupleEntry): GenericRecord = {
    val builder = new GenericRecordBuilder(schemaLocker.get)
    toMap(entry).foreach { kv => builder.set(kv._1, kv._2) }
    builder.build()
  }
}
