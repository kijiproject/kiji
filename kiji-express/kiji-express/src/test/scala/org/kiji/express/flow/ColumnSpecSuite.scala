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

package org.kiji.express.flow

import org.apache.avro.Schema
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.kiji.express.flow.SchemaSpec.Generic

@RunWith(classOf[JUnitRunner])
class ColumnSpecSuite extends FunSuite {
  val filterSpec = ColumnFilterSpec.Regex(".*")
  val colFamily = "myfamily"
  val colQualifier = "myqualifier"
  val qualifierSelector = 'qualifierSym
  val schema = Some(Schema.create(Schema.Type.LONG))

  // TODO(CHOP-37): Test with different filters once the new method of specifying filters
  // correctly implements the .equals() and hashCode() methods.
  // Should be able to change the following line to:
  // def filterSpec = new RegexQualifierColumnFilter(".*")
  val maxVersions = 2

  val colWithOptions: QualifiedColumnInputSpec = QualifiedColumnInputSpec.builder
      .withFamily(colFamily)
      .withQualifier(colQualifier)
      .withMaxVersions(maxVersions)
      .withFilterSpec(filterSpec)
      .build

  test("Fields of a ColumnFamilyInputSpec are the same as those it is constructed with.") {
    val col: ColumnFamilyInputSpec = ColumnFamilyInputSpec.builder
        .withFamily(colFamily)
        .build
    assert(colFamily === col.family)
  }

  test("ColumnInputSpec factory method creates ColumnFamilyInputSpec.") {
    val col = ColumnInputSpec(colFamily)
    assert(col.isInstanceOf[ColumnFamilyInputSpec])
    assert(colFamily === col.asInstanceOf[ColumnFamilyInputSpec].family)
  }

  test("Fields of a ColumnFamilyOutputSpec are the same as those it is constructed with.") {
    val col: ColumnFamilyOutputSpec = ColumnFamilyOutputSpec(colFamily, qualifierSelector)
    assert(colFamily === col.family)
    assert(qualifierSelector === col.qualifierSelector)
    assert(None === col.schemaSpec.schema)
  }

  test("ColumnFamilyOutputSpec factory method creates ColumnFamilyOutputSpec.") {
    val col = ColumnFamilyOutputSpec(colFamily, qualifierSelector, Generic(schema.get))

    assert(colFamily === col.family)
    assert(qualifierSelector === qualifierSelector)
    assert(schema === col.schemaSpec.schema)
  }

  test("Fields of a QualifiedColumnInputSpec are the same as those it is constructed with.") {
    val col: QualifiedColumnInputSpec = QualifiedColumnInputSpec(colFamily, colQualifier)
    assert(colFamily === col.family)
    assert(colQualifier === col.qualifier)
  }

  test("ColumnInputSpec factory method creates QualifiedColumnInputSpec.") {
    val col = QualifiedColumnInputSpec(colFamily, colQualifier)
    assert(col.isInstanceOf[QualifiedColumnInputSpec])
    assert(colFamily === col.asInstanceOf[QualifiedColumnInputSpec].family)
    assert(colQualifier === col.asInstanceOf[QualifiedColumnInputSpec].qualifier)
  }

  test("Fields of a QualifiedColumnOutputSpec are the same as those it is constructed with.") {
    val col: QualifiedColumnOutputSpec =
        QualifiedColumnOutputSpec(colFamily, colQualifier)

    assert(colFamily === col.family)
    assert(colQualifier === col.qualifier)
    assert(None === col.schemaSpec.schema)
  }

  test("QualifiedColumnOutputSpec factory method creates QualifiedColumnOutputSpec.") {
    val col = QualifiedColumnOutputSpec.builder
        .withColumn(colFamily, colQualifier)
        .withSchemaSpec(Generic(schema.get))
        .build
    assert(colQualifier === col.qualifier)
    assert(colFamily === col.family)
    assert(schema === col.schemaSpec.schema)
  }

  test("Two ColumnFamilys with the same parameters are equal and hash to the same value.") {
    val col1 = ColumnFamilyInputSpec.builder.withFamily(colFamily).build
    val col2 = ColumnFamilyInputSpec.builder.withFamily(colFamily).build

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("Two qualified columns with the same parameters are equal and hash to the same value.") {
    val col1 = QualifiedColumnInputSpec.builder.withColumn(colFamily, colQualifier).build
    val col2 = QualifiedColumnInputSpec.builder.withColumn(colFamily, colQualifier).build

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("maxVersions is the same as constructed with.") {
    assert(maxVersions == colWithOptions.maxVersions)
  }

  test("Default maxVersions is 1.") {
    assert(1 == QualifiedColumnInputSpec(colFamily, colQualifier).maxVersions)
  }

  test("Filter is the same as constructed with.") {
    assert(filterSpec == colWithOptions.filterSpec)
  }

  test("ColumnInputSpec with the same maxVersions & filter are equal and hash to the same value.") {

    val col2: QualifiedColumnInputSpec = QualifiedColumnInputSpec.builder
        .withColumn(colFamily, colQualifier)
        .withFilterSpec(filterSpec)
        .withMaxVersions(maxVersions)
        .build
    assert(col2 === colWithOptions)
    assert(col2.hashCode() === colWithOptions.hashCode())
  }
}
