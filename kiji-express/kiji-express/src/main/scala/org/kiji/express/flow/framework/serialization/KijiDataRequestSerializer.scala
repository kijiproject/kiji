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

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder

class KijiDataRequestSerializer extends Serializer[KijiDataRequest] {
  setAcceptsNull(true)

  override def write(kryo: Kryo, output: Output, kijiDataRequest: KijiDataRequest): Unit = {
    kryo.writeClassAndObject(output, kijiDataRequest.getColumns().asScala.toList)
    kryo.writeClassAndObject(output, kijiDataRequest.getMinTimestamp())
    kryo.writeClassAndObject(output, kijiDataRequest.getMaxTimestamp())
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[KijiDataRequest]): KijiDataRequest = {
    val columns = kryo.readClassAndObject(input).asInstanceOf[List[KijiDataRequest.Column]]
    val minTs = kryo.readClassAndObject(input).asInstanceOf[Long]
    val maxTs = kryo.readClassAndObject(input).asInstanceOf[Long]

    val builder: KijiDataRequestBuilder = KijiDataRequest.builder()
    columns.foreach { column: KijiDataRequest.Column =>
      builder.addColumns(builder.newColumnsDef(column))
    }

    builder
      .withTimeRange(minTs, maxTs)
      .build()
  }
}
