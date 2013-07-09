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

import org.kiji.express.avro._
import org.kiji.express.datarequest.AndFilter
import org.kiji.express.datarequest.ColumnRangeFilter
import org.kiji.express.datarequest.ExpressColumnFilter
import org.kiji.express.datarequest.ExpressColumnRequest
import org.kiji.express.datarequest.ExpressDataRequest
import org.kiji.express.datarequest.OrFilter
import org.kiji.express.datarequest.RegexQualifierFilter
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.resourceAsString
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.filter.AndColumnFilter
import org.kiji.schema.filter.Filters
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.KijiColumnRangeFilter
import org.kiji.schema.filter.OrColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.ToJson

class ModelEnvironmentSuite extends FunSuite {
  val validDefinitionLocation: String =
      "src/test/resources/modelEnvironments/valid-model-environment.json"
  val invalidVersionDefinitionLocation: String =
      "src/test/resources/modelEnvironments/invalid-version-model-environment.json"
  val invalidNameDefinitionLocation: String =
      "src/test/resources/modelEnvironments/invalid-name-model-environment.json"
  val invalidProtocolDefinitionLocation: String =
      "src/test/resources/modelEnvironments/invalid-protocol-version-model-environment.json"
  val invalidNameAndVersionDefinitionLocation: String =
      "src/test/resources/modelEnvironments/invalid-name-and-version-model-environment.json"
  val invalidColumnsDefinitionLocation: String =
      "src/test/resources/modelEnvironments/invalid-columns-model-environment.json"
  val validFiltersDefinitionLocation: String =
      "src/test/resources/modelEnvironments/valid-filters-model-environment.json"

  // Expected error messages for validation tests.
  val expectedNameError = "The name of the model environment cannot be the empty string."
  val expectedVersionError =
      "Model environment version strings must match the regex \"[0-9]+(.[0-9]+)*\" " +
      "(1.0.0 would be valid)."
  val expectedProtocolVersionError =
      "\"model_environment-0.1.0\" is the maximum protocol version supported. " +
      "The provided model environment is of protocol version: \"model_environment-0.2.0\""

  test("ModelEnvironment can be created from a path to a valid JSON file.") {
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

    val environment: ModelEnvironment = ModelEnvironment.fromJsonFile(validDefinitionLocation)

    assert("myRunProfile" === environment.name)
    assert("1.0.0" === environment.version)
    assert(expectedRequest === environment.extractEnvironment.dataRequest.toKijiDataRequest())
    assert(expectedKvstores === environment.extractEnvironment.kvstores)
    assert("info:out" === environment.scoreEnvironment.outputColumn)
    assert(expectedKvstores === environment.scoreEnvironment.kvstores)
  }

  test("Settings on a model environment can be modified.") {
    val dataRequest: ExpressDataRequest = new ExpressDataRequest(0, 38475687,
      new ExpressColumnRequest("info:in", 3, None) :: Nil)

    // Extract and score environments to use in tests.
    val extractEnv = ExtractEnvironment(dataRequest, Seq(), Seq())
    val extractEnv2 = ExtractEnvironment(
        dataRequest,
        Seq(FieldBindingSpec("tuplename", "info:storefieldname")),
        Seq(KVStoreSpec("AVRO_KV", "storename", Map("path" -> "/some/great/path"))))
    val scoreEnv = ScoreEnvironment("outputFamily:qualifier", Seq())
    val scoreEnv2 = ScoreEnvironment(
        "outputFamily:qualifier",
        Seq(KVStoreSpec("KIJI_TABLE", "myname", Map("uri" -> "kiji://.env/default/table",
            "column" -> "info:email"))))

    val modelEnv = ModelEnvironment(
      "myname",
      "1.0.0",
      "kiji://myuri",
      extractEnv,
      scoreEnv)

    val modelEnv2 = modelEnv.withNewSettings(name = "newname")
    val modelEnv3 = modelEnv2.withNewSettings(extractEnvironment = extractEnv2)
    val modelEnv4 = modelEnv3.withNewSettings(scoreEnvironment = scoreEnv2)
    val modelEnv5 = modelEnv4.withNewSettings(version = "2.0.0")

    assert(modelEnv.name === "myname")
    assert(modelEnv2.name === "newname")
    assert(modelEnv2.version === modelEnv.version)
    assert(modelEnv2.extractEnvironment === modelEnv.extractEnvironment)
    assert(modelEnv2.scoreEnvironment === modelEnv.scoreEnvironment)

    assert(modelEnv3.version === modelEnv2.version)
    assert(modelEnv2.extractEnvironment == extractEnv)
    assert(modelEnv3.extractEnvironment === extractEnv2)
    assert(modelEnv3.scoreEnvironment === modelEnv2.scoreEnvironment)

    assert(modelEnv4.version === modelEnv3.version)
    assert(modelEnv4.extractEnvironment === modelEnv3.extractEnvironment)
    assert(modelEnv3.scoreEnvironment === scoreEnv)
    assert(modelEnv4.scoreEnvironment === scoreEnv2)

    assert(modelEnv4.version === "1.0.0")
    assert(modelEnv5.version === "2.0.0")
    assert(modelEnv5.name === modelEnv4.name)
    assert(modelEnv5.extractEnvironment === modelEnv4.extractEnvironment)
    assert(modelEnv5.scoreEnvironment === modelEnv4.scoreEnvironment)
  }

  test("ModelEnvironment can write out JSON.") {
    val originalJson: String = doAndClose(Source.fromFile(validDefinitionLocation)) { source =>
      source.mkString
    }
    val originalAvroObject: AvroModelEnvironment = FromJson
        .fromJsonString(originalJson, AvroModelEnvironment.SCHEMA$)
        .asInstanceOf[AvroModelEnvironment]
    val environment = ModelEnvironment.fromJson(originalJson)
    val newJson: String = environment.toJson()
    assert(ToJson.toAvroJsonString(originalAvroObject) === newJson)
  }

  test("ModelEnvironment can validate the version.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJsonFile(invalidVersionDefinitionLocation)
    }
    assert(expectedVersionError === thrown.getMessage)
  }

  test("ModelEnvironment can validate the name.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJsonFile(invalidNameDefinitionLocation)
    }
    assert(expectedNameError === thrown.getMessage)
  }

  test("ModelEnvironment can validate the protocol version.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJsonFile(invalidProtocolDefinitionLocation)
    }
    assert(expectedProtocolVersionError === thrown.getMessage)
  }

  test("ModelEnvironment validates the name and version with an error message including both.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJsonFile(invalidNameAndVersionDefinitionLocation)
    }
    assert(thrown.getMessage.contains(expectedVersionError))
    assert(thrown.getMessage.contains(expectedNameError))
  }

  test("ModelEnvironment validates column names when constructed from JSON.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJsonFile(invalidColumnsDefinitionLocation)
    }
    assert(thrown.getMessage.contains("*invalid1"))
    assert(thrown.getMessage.contains("*invalid2"))
    assert(!thrown.getMessage.contains("valid:column"))
  }

  test("ModelEnvironment validates extract field bindings when programmatically constructed.") {
    val dataRequest: ExpressDataRequest = new ExpressDataRequest(0, 38475687,
        new ExpressColumnRequest("info:in", 3, None) :: Nil)

    val extractEnv = ExtractEnvironment(
        dataRequest,
        Seq(
            FieldBindingSpec("tuplename", "info:storefieldname"),
            FieldBindingSpec("tuplename", "info:storefield2"),
            FieldBindingSpec("tuplename2", "*invalidcolumnname")),
        Seq(KVStoreSpec("AVRO_KV", "storename", Map("path" -> "/some/great/path"))))
    val scoreEnv = ScoreEnvironment(
        "outputFamily:qualifier",
        Seq(KVStoreSpec("KIJI_TABLE", "myname", Map("uri" -> "kiji://.env/default/table",
            "column" -> "info:email"))))

    val thrown = intercept[ModelEnvironmentValidationException] { ModelEnvironment(
      "myname",
      "1.0.0",
      "kiji://myuri",
      extractEnv,
      scoreEnv)
    }

    assert(thrown.getMessage.contains("tuplename"))
    assert(thrown.getMessage.contains("*invalidcolumnname"))
  }

  test("ModelEnvironment can convert an Avro data request to a Kiji data request with a" +
      " null filter.") {
    val colSpec: ColumnSpec = ColumnSpec
      .newBuilder()
      .setName("info:null")
      .build()

    // Build an Avro data request.
    val avroDataReq = AvroDataRequest
      .newBuilder()
      .setMinTimestamp(0L)
      .setMaxTimestamp(38475687)
      .setColumnDefinitions(List(colSpec).asJava)
      .build()

    // Convert from Avro to Kiji.
    val kijiDataReq = ModelEnvironment.avroToKijiDataRequest(avroDataReq)

    // Check that properties are the same.
    assert(avroDataReq.getMinTimestamp === kijiDataReq.getMinTimestamp)
    assert(avroDataReq.getMaxTimestamp === kijiDataReq.getMaxTimestamp)
    val colReq: KijiDataRequest.Column = kijiDataReq.getColumn("info", "null")
    assert("info:null" === colReq.getColumnName.getName)
    assert(1 === colReq.getMaxVersions)
  }

  test("ModelEnvironment can convert an Avro data request to a Kiji data request with a" +
      " column range filter.") {
    val colSpec: ColumnSpec = ColumnSpec
      .newBuilder()
      .setName("info:columnRangeFilter")
      .setFilter(ColumnRangeFilterSpec
        .newBuilder()
        .setMinQualifier(null)
        .setMinIncluded(true)
        .setMaxQualifier(null)
        .setMaxIncluded(true)
        .build()
      )
      .build()

    // Build an Avro data request.
    val avroDataReq = AvroDataRequest
      .newBuilder()
      .setMinTimestamp(0L)
      .setMaxTimestamp(38475687)
      .setColumnDefinitions(List(colSpec).asJava)
      .build()

    // Convert from Avro to Kiji.
    val kijiDataReq = ModelEnvironment.avroToKijiDataRequest(avroDataReq)

    // Check that properties are the same.
    assert(avroDataReq.getMinTimestamp === kijiDataReq.getMinTimestamp)
    assert(avroDataReq.getMaxTimestamp === kijiDataReq.getMaxTimestamp)
    val colReq: KijiDataRequest.Column = kijiDataReq.getColumn("info", "columnRangeFilter")
    assert("info:columnRangeFilter" === colReq.getColumnName.getName)
    assert(1 === colReq.getMaxVersions)
    assert(colReq.getFilter.isInstanceOf[KijiColumnRangeFilter], "incorrect filter type")
  }

  test("ModelEnvironment can convert an Avro data request to a Kiji data request with a" +
      " regex qualifier filter.") {
    val colSpec: ColumnSpec = ColumnSpec
      .newBuilder()
      .setName("info:regexQualifierFilter")
      .setFilter(RegexQualifierFilterSpec
        .newBuilder()
        .setRegex("hello")
        .build()
      )
      .build()

    // Build an Avro data request.
    val avroDataReq = AvroDataRequest
      .newBuilder()
      .setMinTimestamp(0L)
      .setMaxTimestamp(38475687)
      .setColumnDefinitions(List(colSpec).asJava)
      .build()

    // Convert from Avro to Kiji.
    val kijiDataReq = ModelEnvironment.avroToKijiDataRequest(avroDataReq)

    // Check that properties are the same.
    assert(avroDataReq.getMinTimestamp === kijiDataReq.getMinTimestamp)
    assert(avroDataReq.getMaxTimestamp === kijiDataReq.getMaxTimestamp)
    val colReq: KijiDataRequest.Column = kijiDataReq.getColumn("info", "regexQualifierFilter")
    assert("info:regexQualifierFilter" === colReq.getColumnName.getName)
    assert(1 === colReq.getMaxVersions)
    assert(colReq.getFilter.isInstanceOf[RegexQualifierColumnFilter], "incorrect filter type")
  }

  test("ModelEnvironment can convert an Avro data request to a Kiji data request with a" +
      " logical AND filter.") {
    val columnRangeFilterSpec = ColumnRangeFilterSpec
      .newBuilder()
      .setMinQualifier(null)
      .setMinIncluded(true)
      .setMaxQualifier(null)
      .setMaxIncluded(true)
      .build()
    val regexQualifierFilterSpec = RegexQualifierFilterSpec
      .newBuilder()
      .setRegex("hello")
      .build()

    val colSpec: ColumnSpec = ColumnSpec
      .newBuilder()
      .setName("info:andFilter")
      .setFilter(AndFilterSpec
        .newBuilder()
        .setAndFilters(List(columnRangeFilterSpec.asInstanceOf[java.lang.Object],
            regexQualifierFilterSpec.asInstanceOf[java.lang.Object]).asJava)
        .build()
      )
      .build()

    // Build an Avro data request.
    val avroDataReq = AvroDataRequest
      .newBuilder()
      .setMinTimestamp(0L)
      .setMaxTimestamp(38475687)
      .setColumnDefinitions(List(colSpec).asJava)
      .build()

    // Convert from Avro to Kiji.
    val kijiDataReq = ModelEnvironment.avroToKijiDataRequest(avroDataReq)

    // Check that properties are the same.
    assert(avroDataReq.getMinTimestamp === kijiDataReq.getMinTimestamp)
    assert(avroDataReq.getMaxTimestamp === kijiDataReq.getMaxTimestamp)
    val colReq: KijiDataRequest.Column = kijiDataReq.getColumn("info", "andFilter")
    assert("info:andFilter" === colReq.getColumnName.getName)
    assert(1 === colReq.getMaxVersions)
    assert(colReq.getFilter.isInstanceOf[AndColumnFilter], "incorrect filter type")
  }

  test("ModelEnvironment can convert an Avro data request to a Kiji data request with a" +
      " logical OR filter.") {
    val columnRangeFilterSpec = ColumnRangeFilterSpec
      .newBuilder()
      .setMinQualifier(null)
      .setMinIncluded(true)
      .setMaxQualifier(null)
      .setMaxIncluded(true)
      .build()
    val regexQualifierFilterSpec = RegexQualifierFilterSpec
      .newBuilder()
      .setRegex("hello")
      .build()

    val colSpec: ColumnSpec = ColumnSpec
      .newBuilder()
      .setName("info:orFilter")
      .setFilter(OrFilterSpec
        .newBuilder()
        .setOrFilters(List(columnRangeFilterSpec.asInstanceOf[java.lang.Object],
            regexQualifierFilterSpec.asInstanceOf[java.lang.Object]).asJava)
        .build()
      )
      .build()

    // Build an Avro data request.
    val avroDataReq = AvroDataRequest
      .newBuilder()
      .setMinTimestamp(0L)
      .setMaxTimestamp(38475687)
      .setColumnDefinitions(List(colSpec).asJava)
      .build()

    // Convert from Avro to Kiji.
    val kijiDataReq = ModelEnvironment.avroToKijiDataRequest(avroDataReq)

    // Check that properties are the same.
    assert(avroDataReq.getMinTimestamp === kijiDataReq.getMinTimestamp)
    assert(avroDataReq.getMaxTimestamp === kijiDataReq.getMaxTimestamp)
    val colReq: KijiDataRequest.Column = kijiDataReq.getColumn("info", "orFilter")
    assert("info:orFilter" === colReq.getColumnName.getName)
    assert(1 === colReq.getMaxVersions)
    assert(colReq.getFilter.isInstanceOf[OrColumnFilter], "incorrect filter type")
  }

  test("ModelEnvironment can instantiate an Express column filter from an Avro column filter.") {
    val columnRangeFilterSpec = ColumnRangeFilterSpec
      .newBuilder()
      .setMinQualifier(null)
      .setMinIncluded(true)
      .setMaxQualifier(null)
      .setMaxIncluded(true)
      .build()
    val regexQualifierFilterSpec = RegexQualifierFilterSpec
      .newBuilder()
      .setRegex("hello")
      .build()
    val andFilterSpec = AndFilterSpec
      .newBuilder()
      .setAndFilters(List(columnRangeFilterSpec.asInstanceOf[java.lang.Object],
          regexQualifierFilterSpec.asInstanceOf[java.lang.Object]).asJava)
      .build()

    val expressFilter: ExpressColumnFilter = ExpressDataRequest.filterFromAvro(andFilterSpec)
    assert(expressFilter.isInstanceOf[AndFilter], "incorrect filter instantiated")
  }

  test("ModelEnvironment can instantiate an Avro column filter from an Express column filter.") {
    val expressRegexFilter: ExpressColumnFilter = new RegexQualifierFilter("hello")
    val avroRegexFilter: AnyRef =
        ExpressColumnFilter.expressToAvroFilter(expressRegexFilter)

    // Check that properties are the same.
    assert(avroRegexFilter.isInstanceOf[RegexQualifierFilterSpec], "incorrect filter instantiated")
    assert("hello" === avroRegexFilter.asInstanceOf[RegexQualifierFilterSpec].getRegex)
  }

  test("ModelEnvironment can instantiate Kiji column filters from json.") {
    val modelEnv: ModelEnvironment = ModelEnvironment.fromJsonFile(validFiltersDefinitionLocation)

    val expRegexFilter: RegexQualifierFilter = new RegexQualifierFilter("foo")
    val expColRangeFilter = new ColumnRangeFilter("null", true, "null", true)
    val expAndFilter: AndFilter = new AndFilter(List(expRegexFilter, expColRangeFilter))

    val expectedRequest: ExpressDataRequest = new ExpressDataRequest(0, 38475687,
        new ExpressColumnRequest("info:in", 3, Some(expAndFilter)) :: Nil)

    assert(expectedRequest === modelEnv.extractEnvironment.dataRequest)
  }

  // Integration test for Kiji column filters.
  test("ModelEnvironment can validate a Kiji data request from json containing column filters.") {
    // Create a model environment from json.
    val modelEnv: ModelEnvironment = ModelEnvironment.fromJsonFile(validFiltersDefinitionLocation)

    // Pull out the Kiji data request to test from the model environment.
    val expDataReq: ExpressDataRequest = modelEnv.extractEnvironment.dataRequest
    val kijiDataReq: KijiDataRequest = expDataReq.toKijiDataRequest()

    // Programmatically create the expected Kiji data request (with filters).
    val kijiRegexFilter: RegexQualifierColumnFilter = new RegexQualifierColumnFilter("foo")
    val kijiColRangeFilter: KijiColumnRangeFilter =
        new KijiColumnRangeFilter("null", true, "null", true)
    val arr = Array(kijiRegexFilter, kijiColRangeFilter)
    val kijiAndFilter: KijiColumnFilter = Filters.and(arr: _*)

    val kijiDataRequest: KijiDataRequest = KijiDataRequest.builder()
        .withTimeRange(0, 38475687)
        .addColumns(KijiDataRequestBuilder.ColumnsDef
            .create()
            .withMaxVersions(3)
            .withFilter(kijiAndFilter)
            .add(new KijiColumnName("info:in"))
        )
        .build()

    // Test for the expected Kiji data request containing the expected column filters.
    assert(kijiDataRequest === kijiDataReq)
  }
}
