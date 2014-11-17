package org.kiji.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.serializer.KryoRegistrator

import org.kiji.express.flow.framework.serialization.AvroSpecificSerializer
import org.kiji.express.flow.framework.serialization.KijiDataRequestSerializer
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.impl.MaterializedKijiResult

/**
 * Registers serializer classes for a SparkJob
 * @tparam T type of MaterializedKijiResult
 */
class KijiSparkRegistrator[T] extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.addDefaultSerializer(classOf[SpecificRecord], classOf[AvroSpecificSerializer])
    kryo.register(classOf[MaterializedKijiResult[T]], new MaterializedKijiResultSerializer[T]())
    kryo.register(classOf[KijiDataRequest], new KijiDataRequestSerializer())
    kryo.register(classOf[KijiCell[T]], new KijiCellSerializer())
  }
}
