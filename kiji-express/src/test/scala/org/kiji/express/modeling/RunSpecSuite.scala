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

package org.kiji.express.modeling

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

import org.scalatest.FunSuite

import org.kiji.express.Resources.doAndClose
import org.kiji.express.Resources.resourceAsString
import org.kiji.express.avro.AvroDataRequest
import org.kiji.express.avro.AvroRunSpec
import org.kiji.express.avro.ColumnSpec
import org.kiji.express.avro.KVStore
import org.kiji.express.avro.KvStoreType
import org.kiji.express.avro.Property
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.ToJson

class RunSpecSuite extends FunSuite {
  val validSpecLocation: String = "src/test/resources/runSpecs/valid-run-spec.json"
  val invalidVersionSpecLocation: String =
      "src/test/resources/runSpecs/invalid-version-run-spec.json"
  val invalidNameSpecLocation: String =
      "src/test/resources/runSpecs/invalid-name-run-spec.json"
  val invalidProtocolSpecLocation: String =
      "src/test/resources/runSpecs/invalid-protocol-version-run-spec.json"

  test("RunSpec can be created from a path to a valid JSON file.") {
    val expectedRequest: KijiDataRequest = {
      val builder = KijiDataRequest.builder().withTimeRange(0, 38475687)
      builder.newColumnsDef().withMaxVersions(3).add("info", "in")
      builder.build()
    }

    val expectedKvstores: Seq[KVStore] = {
      val property: Property = Property
          .newBuilder()
          .setName("path")
          .setValue("/usr/src/and/so/on")
          .build()
      val store: KVStore = KVStore
          .newBuilder()
          .setName("side_data")
          .setStoreType(KvStoreType.AVRO_KV)
          .setProperties(Seq(property).asJava)
          .build()

      Seq(store)
    }

    val spec: RunSpec = RunSpec.fromJsonFile(validSpecLocation)

    assert("myRunProfile" === spec.name)
    assert("1.0.0" === spec.version)
    assert(expectedRequest === spec.extractRunSpec.dataRequest)
    assert(expectedKvstores === spec.extractRunSpec.kvstores)
    assert("info:out" === spec.scoreRunSpec.outputColumn)
    assert(expectedKvstores === spec.scoreRunSpec.kvstores)
  }

  test("RunSpec can write out JSON.") {
    val originalJson: String = doAndClose(Source.fromFile(validSpecLocation)) { source =>
      source.mkString
    }
    val originalAvroObject: AvroRunSpec = FromJson
        .fromJsonString(originalJson, AvroRunSpec.SCHEMA$)
        .asInstanceOf[AvroRunSpec]
    val spec = RunSpec.fromJson(originalJson)
    val newJson: String = spec.toJson()
    assert(ToJson.toAvroJsonString(originalAvroObject) === newJson)
  }

  test("RunSpec can validate the version.") {
    val thrown = intercept[RunSpecValidationException] {
      RunSpec.fromJsonFile(invalidVersionSpecLocation)
    }
    assert("Run spec version strings must match the regex \"[0-9]+(.[0-9]+)*\" " +
        "(1.0.0 would be valid)." === thrown.getMessage)
  }

  test("RunSpec can validate the name.") {
    val thrown = intercept[RunSpecValidationException] {
      RunSpec.fromJsonFile(invalidNameSpecLocation)
    }
    assert("The name of the run spec can not be the empty string." === thrown.getMessage)
  }

  test("RunSpec can validate the protocol version.") {
    val thrown = intercept[RunSpecValidationException] {
      RunSpec.fromJsonFile(invalidProtocolSpecLocation)
    }
    assert("\"run_spec-0.1.0\" is the maximum protocol version supported. " +
        "The provided run spec is of protocol version: \"run_spec-0.2.0\"" === thrown.getMessage)
  }
}
