/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.spark.connector.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.avro.Schema

import org.kiji.schema.DecodedCell
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiColumnName

/**
 * Kryo serializer for [[org.kiji.schema.KijiCell]].
 */
class KijiCellSerializer[T] extends Serializer[KijiCell[T]] {
  override def write(
                      kryo: Kryo,
                      output: Output,
                      kijiCell: KijiCell[T]): Unit = {
    kryo.writeClassAndObject(output, kijiCell.getColumn.getFamily)
    kryo.writeClassAndObject(output, kijiCell.getColumn.getQualifier)
    kryo.writeClassAndObject(output, kijiCell.getTimestamp)
    kryo.writeClassAndObject(output, kijiCell.getData)
    kryo.writeClassAndObject(output, kijiCell.getReaderSchema)
    kryo.writeClassAndObject(output, kijiCell.getWriterSchema)
  }

  override def read(
                     kryo: Kryo,
                     input: Input,
                     clazz: Class[KijiCell[T]]): KijiCell[T] = {
    val family: String = kryo.readClassAndObject(input).asInstanceOf[String]
    val qualifier: String = kryo.readClassAndObject(input).asInstanceOf[String]
    val timeStamp: Long = kryo.readClassAndObject(input).asInstanceOf[Long]
    val data: T = kryo.readClassAndObject(input).asInstanceOf[T]
    val readerSchema: Schema = kryo.readClassAndObject(input).asInstanceOf[Schema]
    val writerSchema: Schema = kryo.readClassAndObject(input).asInstanceOf[Schema]
    KijiCell.create[T](KijiColumnName.create(family, qualifier), timeStamp,
      new DecodedCell[T](writerSchema, readerSchema, data))

  }
}