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

package org.kiji.express.flow.framework.serialization

import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.hbase.HBaseKijiTable
import org.kiji.schema.impl.hbase.HBaseKijiRowData

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.hadoop.hbase.client.Result

/**
 * Kryo serializer for [[org.kiji.schema.impl.hbase.HBaseKijiRowData]].
 */
class HBaseKijiRowDataSerializer extends Serializer[HBaseKijiRowData] {
  override def write(kryo: Kryo, output: Output, kijiRowData: HBaseKijiRowData): Unit = {
    // Write the KijiURI as a string.  Unfortunately using Kryo built-in serialization for it
    // leads to errors because it cannot modify an underlying immutable collection.
    kryo.writeClassAndObject(output, kijiRowData.getTable().getURI)
    kryo.writeClassAndObject(output, kijiRowData.getDataRequest())
    kryo.writeClassAndObject(output, kijiRowData.getEntityId())
    kryo.writeClassAndObject(output, kijiRowData.getHBaseResult())
    // Do not attempt to write the CellDecoderProvider.  It can get created again on the other
    // side.
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[HBaseKijiRowData]): HBaseKijiRowData = {
    val kijiURI: KijiURI = kryo.readClassAndObject(input).asInstanceOf[KijiURI]
    val dataRequest: KijiDataRequest = kryo.readClassAndObject(input).asInstanceOf[KijiDataRequest]
    val entityId: EntityId = kryo.readClassAndObject(input).asInstanceOf[EntityId]
    val result: Result = kryo.readClassAndObject(input).asInstanceOf[Result]
    val table: HBaseKijiTable =
      HBaseKijiTable.downcast(Kiji.Factory.get().open(kijiURI).openTable(kijiURI.getTable))

    // Initialize a new HBaseKijiRowData.  It will create a new CellDecoderProvider since we pass
    // in null.
    new HBaseKijiRowData(table, dataRequest, entityId, result, null)
  }
}
