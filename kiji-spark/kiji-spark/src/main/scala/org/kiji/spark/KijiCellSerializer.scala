package org.kiji.spark

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
    val  qualifier: String = kryo.readClassAndObject(input).asInstanceOf[String]
    val  timeStamp: Long = kryo.readClassAndObject(input).asInstanceOf[Long]
    val  data: T = kryo.readClassAndObject(input).asInstanceOf[T]
    val  readerSchema: Schema = kryo.readClassAndObject(input).asInstanceOf[Schema]
    val  writerSchema: Schema = kryo.readClassAndObject(input).asInstanceOf[Schema]
    KijiCell.create[T](KijiColumnName.create(family, qualifier), timeStamp,
      new DecodedCell[T](writerSchema, readerSchema, data))

  }
}