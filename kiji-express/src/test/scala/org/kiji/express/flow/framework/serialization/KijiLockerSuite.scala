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

package org.kiji.express.flow.framework.serialization

import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream

import org.apache.avro.Schema
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import org.kiji.express.KijiSuite

@RunWith(classOf[JUnitRunner])
class KijiLockerSuite extends KijiSuite {

  test("KijiLocker can serialize and deserialize a Schema.") {
    val schema = Schema.create(Schema.Type.STRING)

    val copy = lockerRoundtrip(schema)

    assert(schema === copy)
    assert(schema.toString(false) === copy.toString(false))
  }

  def lockerRoundtrip[T <: AnyRef](value: T): T = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(KijiLocker(value))

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

    ois.readObject().asInstanceOf[KijiLocker[T]].get
  }
}

