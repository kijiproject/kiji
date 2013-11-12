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

package org.kiji.modeling.config

import scala.collection.JavaConverters._


import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.express.flow.AndFilterSpec
import org.kiji.express.flow.Between
import org.kiji.express.flow.ColumnRangeFilterSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ColumnFilterSpec
import org.kiji.express.flow.OrFilterSpec
import org.kiji.express.flow.PagingSpec
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.RegexQualifierFilterSpec
import org.kiji.express.util.Resources.resourceAsString
import org.kiji.modeling.avro.AvroColumnFamilyInputSpec
import org.kiji.modeling.avro.AvroColumnFamilyOutputSpec
import org.kiji.modeling.avro.AvroColumnRangeFilter
import org.kiji.modeling.avro.AvroFilter
import org.kiji.modeling.avro.AvroInputFieldBinding
import org.kiji.modeling.avro.AvroKijiInputSpec
import org.kiji.modeling.avro.AvroModelEnvironment
import org.kiji.modeling.avro.AvroOutputFieldBinding
import org.kiji.modeling.avro.AvroQualifiedColumnInputSpec
import org.kiji.modeling.avro.AvroQualifiedColumnOutputSpec
import org.kiji.modeling.avro.AvroRegexQualifierFilter
import org.kiji.modeling.avro.AvroTimeRange
import org.kiji.modeling.framework.ModelConverters
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.filter.Filters
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.KijiColumnRangeFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.ToJson

@RunWith(classOf[JUnitRunner])
class ModelEnvironmentSuite extends FunSuite {
  val validDefinitionLocation: String = resourceAsString(
      "modelEnvironments/valid-model-environment.json")

  val invalidVersionDefinitionLocation: String = resourceAsString(
      "modelEnvironments/invalid-version-model-environment.json")

  val invalidNameDefinitionLocation: String = resourceAsString(
      "modelEnvironments/invalid-name-model-environment.json")

  val invalidProtocolDefinitionLocation: String = resourceAsString(
      "modelEnvironments/invalid-protocol-version-model-environment.json")

  val invalidNameAndVersionDefinitionLocation: String = resourceAsString(
      "modelEnvironments/invalid-name-and-version-model-environment.json")

  val invalidColumnsDefinitionLocation: String = resourceAsString(
      "modelEnvironments/invalid-columns-model-environment.json")

  val validFiltersDefinitionLocation: String = resourceAsString(
      "modelEnvironments/valid-filters-model-environment.json")

  val invalidPrepareEnvironmentLocation: String = resourceAsString(
      "modelEnvironments/invalid-prepare-model-environment.json")

  val invalidTrainEnvironmentLocation: String = resourceAsString(
      "modelEnvironments/invalid-train-model-environment.json")

  val invalidEvaluateEnvironmentLocation: String = resourceAsString(
    "modelEnvironments/invalid-evaluate-model-environment.json")

  // Expected error messages for validation tests.
  val expectedNameError: String = "The name of the model environment cannot be the empty string."
  val expectedVersionError: String =
      "Model environment version strings must match the regex \"[0-9]+(.[0-9]+)*\" " +
      "(1.0.0 would be valid)."
  val expectedProtocolVersionError: String =
      "\"model_environment-0.4.0\" is the maximum protocol version supported. " +
      "The provided model environment is of protocol version: \"model_environment-7.3.0\""

  test("ModelEnvironment can be created from a path to a valid JSON file.") {
    val expectedRequest: KijiDataRequest = {
      val builder = KijiDataRequest.builder().withTimeRange(0, 38475687)
      builder.newColumnsDef().withMaxVersions(3).add("info", "in")
      builder.build()
    }

    val expectedKvstores: Seq[KeyValueStoreSpec] = Seq(
        KeyValueStoreSpec(
            name="side_data",
            storeType="AVRO_KV",
            properties=Map("path" -> "/usr/src/and/so/on")))

    val environment: ModelEnvironment = ModelEnvironment.fromJson(validDefinitionLocation)

    assert("myRunProfile" === environment.name)
    assert("1.0.0" === environment.version)
    assert(expectedRequest === environment
        .scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]
        .toKijiDataRequest)

    assert(expectedKvstores === environment.scoreEnvironment.get.keyValueStoreSpecs)
    assert("info:out" === environment
        .scoreEnvironment
        .get
        .outputSpec
        .asInstanceOf[KijiSingleColumnOutputSpec]
        .outputColumn
        .columnName
        .toString)
    assert(expectedKvstores === environment.scoreEnvironment.get.keyValueStoreSpecs)
  }

  test("Settings on a model environment can be modified.") {
    val timeRange = Between(0, 38475687)
    val columns = Map(QualifiedColumnInputSpec("info", "in", maxVersions=3) -> 'tuplename)

    // Extract and score environments to use in tests.
    val inputSpec = KijiInputSpec("kiji://myuri", timeRange, columns)
    val inputSpec2 = KijiInputSpec("kiji://myuri", timeRange, columns)

    val outputSpec = KijiSingleColumnOutputSpec(
        "kiji://myuri",
        QualifiedColumnOutputSpec("outputFamily:qualifier"))
    val scoreEnv = Some(ScoreEnvironment(inputSpec, outputSpec, Seq()))
    val scoreEnv2 = Some(ScoreEnvironment(
        inputSpec2,
        outputSpec,
        Seq(KeyValueStoreSpec("KIJI_TABLE", "myname", Map("uri" -> "kiji://.env/default/table",
            "column" -> "info:email")),
            KeyValueStoreSpec("AVRO_KV", "storename", Map("path" -> "/some/great/path")))))

    val modelEnv = ModelEnvironment(
      "myname",
      "1.0.0",
      None,
      None,
      scoreEnv)

    val modelEnv2 = modelEnv.withNewSettings(name = "newname")
    val modelEnv3 = modelEnv2.withNewSettings(scoreEnvironment = scoreEnv2)
    val modelEnv4 = modelEnv3.withNewSettings(version = "7.3.0")

    assert(modelEnv.name === "myname")

    assert(modelEnv2.name === "newname")
    assert(modelEnv2.version === modelEnv.version)
    assert(modelEnv2.scoreEnvironment === modelEnv.scoreEnvironment)

    assert(modelEnv3.version === modelEnv2.version)
    assert(modelEnv3.name === modelEnv2.name)
    assert(modelEnv3.scoreEnvironment === scoreEnv2)

    assert(modelEnv4.scoreEnvironment === modelEnv3.scoreEnvironment)
    assert(modelEnv4.version === "7.3.0")
  }

  test("A ModelEnvironment can be serialized and deserialized again.") {
    val timeRange = Between(0, 38475687)
    val columns = Map(QualifiedColumnInputSpec("info", "in", maxVersions=3) -> 'tuplename)

    val inputSpec = new KijiInputSpec("kiji://.env/default/table", timeRange, columns)
    val outputSpec = new KijiOutputSpec(
        "kiji://.env/default/table",
        Map('tuplename -> QualifiedColumnOutputSpec("info:storefieldname"))
    )
    val scoreOutputSpec = new KijiSingleColumnOutputSpec(
      "kiji://.env/default/table",
      QualifiedColumnOutputSpec("info:scoreoutput")
    )

    // Prepare, extract, train and score environments to use in tests.
    val prepareEnv = PrepareEnvironment(
        Map("input" -> inputSpec),
        Map("output" -> outputSpec),
        Seq(KeyValueStoreSpec("AVRO_KV", "storename", Map("path" -> "/some/great/path")))
    )
    val trainEnv = TrainEnvironment(
        Map("input" -> inputSpec),
        Map("output" -> outputSpec),
        Seq(KeyValueStoreSpec("AVRO_KV", "storename", Map("path" -> "/some/great/path")))
    )
    val scoreEnv = ScoreEnvironment(
        inputSpec,
        scoreOutputSpec,
        Seq(KeyValueStoreSpec("KIJI_TABLE", "myname", Map("uri" -> "kiji://.env/default/table",
            "column" -> "info:email"))))
    val evaluateEnv = EvaluateEnvironment(
        inputSpec,
        outputSpec,
        Seq(KeyValueStoreSpec("AVRO_KV", "storename", Map("path" -> "/some/great/path")))
    )

    val modelEnv = ModelEnvironment(
        "myname",
        "1.0.0",
        Some(prepareEnv),
        Some(trainEnv),
        Some(scoreEnv),
        Some(evaluateEnv))
    val jsonModelEnv: String = modelEnv.toJson
    val returnedModelEnv: ModelEnvironment = ModelEnvironment.fromJson(jsonModelEnv)

    assert(modelEnv === returnedModelEnv)
  }

  test("ModelEnvironment can write out JSON.") {
    val originalAvroObject: AvroModelEnvironment = FromJson
        .fromJsonString(validDefinitionLocation, AvroModelEnvironment.SCHEMA$)
        .asInstanceOf[AvroModelEnvironment]
    val environment = ModelEnvironment.fromJson(validDefinitionLocation)
    val newJson: String = environment.toJson
    assert(ToJson.toAvroJsonString(originalAvroObject) === newJson)
  }

  test("ModelEnvironment can validate the version.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJson(invalidVersionDefinitionLocation)
    }
    assert(expectedVersionError === thrown.getMessage)
  }

  test("ModelEnvironment can validate the name.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJson(invalidNameDefinitionLocation)
    }
    assert(expectedNameError === thrown.getMessage)
  }

  test("ModelEnvironment can validate the protocol version.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJson(invalidProtocolDefinitionLocation)
    }
    assert(expectedProtocolVersionError === thrown.getMessage)
  }

  test("ModelEnvironment validates the name and version with an error message including both.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJson(invalidNameAndVersionDefinitionLocation)
    }
    assert(thrown.getMessage.contains(expectedVersionError))
    assert(thrown.getMessage.contains(expectedNameError))
  }

  test("ModelEnvironment validates column names when constructed from JSON.") {
    val thrown = intercept[KijiInvalidNameException] {
      ModelEnvironment.fromJson(invalidColumnsDefinitionLocation)
    }
  }


  // -----------------------------------------------------------------------------------------------
  // Useful functions to reduce boilerplate in filter tests.
  // Given an optional filter, do the following:
  // - Build stuff
  //     - Create a single avro column called "info:foo"
  //     - If a filter is present, add it
  //     - Create the entire AvroKijiInputSpec
  //     - Build the KijiInputSpec from the AvroKijiInputSpec
  // - Verify stuff
  //     - The time range should be correct
  //     - There should be only one column
  //     - It should have the correct name and field binding
  //     - It should have the correct max versions
  //     - It should have the appropriate filter
  def singleColumnKijiInputSpecToAndFromAvro(
      avroFilter: Option[AvroFilter] = None): Option[ColumnFilterSpec] = {

    // Constants to use for building avro and check after converting from avro
    val myFamily = "info"
    val myQualifier = "foo"
    val myMaxVersions = 1
    val myPageSize = 0 // Will get converted to PagingSpec.Off during transformation from Avro
    val myField = "field"
    val myMinTime = 0L
    val myMaxTime = 38475687L
    val myUri = "kiji://foo"
    val myLoggingInterval = 1000L

    // Build the column
    val avroColumn = AvroQualifiedColumnInputSpec
        .newBuilder()
        .setFamily(myFamily)
        .setQualifier(myQualifier)
        .setMaxVersions(myMaxVersions)
        .setFilter(avroFilter.getOrElse(null))
        .setPageSize(myPageSize)
        .build()

    // Create the field binding for the column
    val avroFieldBinding = AvroInputFieldBinding
        .newBuilder()
        .setColumn(avroColumn)
        .setTupleFieldName(myField)
        .build()

    // Create the time range for the request
    val avroTimeRange = AvroTimeRange
        .newBuilder()
        .setMinTimestamp(myMinTime)
        .setMaxTimestamp(myMaxTime)
        .build()

    // Now create a full avro description of the KijiInputSpec!
    val avroKijiInputSpec = AvroKijiInputSpec
        .newBuilder()
        .setTableUri(myUri)
        .setTimeRange(avroTimeRange)
        .setColumnsToFields(List(avroFieldBinding).asJava)
        .build()


    // Convert from Avro to Kiji.
    val kijiInputSpec = ModelConverters.kijiInputSpecFromAvro(avroKijiInputSpec)

    // Verify settings for the KijiInputSpec
    assert(kijiInputSpec.timeRange.begin === myMinTime)
    assert(kijiInputSpec.timeRange.end === myMaxTime)
    assert(kijiInputSpec.tableUri === myUri)

    // Verify settings for the column / field binding
    assert(1 === kijiInputSpec.columnsToFields.size)

    val field = kijiInputSpec.columnsToFields.values.head
    assert(field.isInstanceOf[Symbol])
    assert(myField === field.name)

    val column = kijiInputSpec.columnsToFields.keys.head.asInstanceOf[QualifiedColumnInputSpec]
    assert(myFamily === column.family)
    assert(myQualifier === column.qualifier)
    assert(myMaxVersions === column.maxVersions)
    assert(PagingSpec.Off === column.paging)

    // Return the filter for further checking
    column.filter
  }

  // -----------------------------------------------------------------------------------------------
  // Useful definitions for filters that we can reuse in tests below

  val myAvroRangeFilter = {
    val internal = AvroColumnRangeFilter
        .newBuilder()
        .setMinQualifier("null")
        .setMinIncluded(true)
        .setMaxQualifier("null")
        .setMaxIncluded(true)
        .build()
    AvroFilter
        .newBuilder()
        .setRangeFilter(internal)
        .build()
  }

  val myAvroRegexQualifierFilter = {
    val internal = AvroRegexQualifierFilter
        .newBuilder()
        .setRegex("hello")
        .build()
    AvroFilter
        .newBuilder()
        .setRegexFilter(internal)
        .build()
  }

  val myAvroOrFilter = AvroFilter
      .newBuilder()
      .setOrFilter(Seq(myAvroRegexQualifierFilter, myAvroRangeFilter).asJava)
      .build()

  val myAvroAndFilter = AvroFilter
      .newBuilder()
      .setAndFilter(Seq(myAvroRegexQualifierFilter, myAvroRangeFilter).asJava)
      .build()

  // -----------------------------------------------------------------------------------------------
  // Actual filter tests

  test("ModelEnvironment can convert from Avro to KijiInputSpec without a filter.") {
    val filter = singleColumnKijiInputSpecToAndFromAvro()
    assert(None === filter)
  }

  test("ModelEnvironment can convert from Avro to KijiInputSpec with a range filter.") {
    val filter = singleColumnKijiInputSpecToAndFromAvro(Some(myAvroRangeFilter))
    assert(filter.get.isInstanceOf[ColumnRangeFilterSpec], "incorrect filter type")
  }

  test("ModelEnvironment can convert from Avro to KijiInputSpec with a regex qualifier filter.") {
    val filter = singleColumnKijiInputSpecToAndFromAvro(Some(myAvroRegexQualifierFilter))
    assert(filter.get.isInstanceOf[RegexQualifierFilterSpec], "incorrect filter type")
  }

  test("ModelEnvironment can convert from Avro to KijiInputSpec with a logical AND filter.") {
    val filter = singleColumnKijiInputSpecToAndFromAvro(Some(myAvroAndFilter))
    assert(filter.get.isInstanceOf[AndFilterSpec], "incorrect filter type")
  }

  test("ModelEnvironment can convert from Avro to KijiInputSpec with a logical OR filter.") {
    val filter = singleColumnKijiInputSpecToAndFromAvro(Some(myAvroOrFilter))
    assert(filter.get.isInstanceOf[OrFilterSpec], "incorrect filter type")
  }

  test("ModelEnvironment can instantiate Kiji column filters from json.") {
    val modelEnv: ModelEnvironment = ModelEnvironment.fromJson(validFiltersDefinitionLocation)

    // Filter definition that should exist in the JSON
    val expectedRegexFilter: RegexQualifierFilterSpec = new RegexQualifierFilterSpec("foo")
    val expectedColRangeFilter = new ColumnRangeFilterSpec(
        minimum = Some("null"),
        maximum = Some("null"),
        minimumIncluded = true,
        maximumIncluded = true)
    val expectedAndFilter = new AndFilterSpec(List(expectedRegexFilter, expectedColRangeFilter))

    val kijiInputSpec = modelEnv
        .scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]

    assert(expectedAndFilter === kijiInputSpec.columnsToFields.keys.toList(0).filter.get)

  }

  // Integration test for Kiji column filters.
  test("ModelEnvironment can validate a Kiji data request from json containing column filters.") {
    // Create a model environment from json.
    val modelEnv: ModelEnvironment = ModelEnvironment.fromJson(validFiltersDefinitionLocation)

    // Pull out the Kiji data request to test from the model environment.
    val expKijiInputSpec: KijiInputSpec = modelEnv.scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]

    val expKijiDataReq: KijiDataRequest = expKijiInputSpec.toKijiDataRequest

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
    assert(kijiDataRequest === expKijiDataReq)
  }

  // -----------------------------------------------------------------------------------------------


  // TODO: Possibly spiffy up these three tests if we determine a better way to handle construction
  // errors for ModelEnvironments.  (After refactoring for EXP-232, errors that previously would
  // have been caught during validation are now caught during construction.)
  test("ModelEnvironment validates prepare environment correctly.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJson(invalidPrepareEnvironmentLocation)
    }
  }

  test("ModelEnvironment validates train environment correctly.") {
    val thrown = intercept[ModelEnvironmentValidationException] {
      ModelEnvironment.fromJson(invalidTrainEnvironmentLocation)
    }
  }

  test("ModelEnvironment validates evaluate environment correctly.") {
    val thrown = intercept[KijiInvalidNameException] {
      ModelEnvironment.fromJson(invalidEvaluateEnvironmentLocation)
    }
  }
}
