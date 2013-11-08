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

@RunWith(classOf[JUnitRunner])
class ColumnRequestSuite extends FunSuite {
  val filter = RegexQualifierFilter(".*")
  val colFamily = "myfamily"
  val colQualifier = "myqualifier"
  val qualifierSelector = 'qualifierSym
  val schema = Some(Schema.create(Schema.Type.LONG))

  // TODO(CHOP-37): Test with different filters once the new method of specifying filters
  // correctly implements the .equals() and hashCode() methods.
  // Should be able to change the following line to:
  // def filter = new RegexQualifierColumnFilter(".*")
  val maxVersions = 2

  val colWithOptions: QualifiedColumnRequestInput = QualifiedColumnRequestInput(
      family = colFamily,
      qualifier = colQualifier,
      maxVersions = maxVersions,
      filter = Some(filter)
  )

  test("Fields of a ColumnFamilyRequestInput are the same as those it is constructed with.") {
    val col: ColumnFamilyRequestInput = new ColumnFamilyRequestInput(family = colFamily)
    assert(colFamily === col.family)
  }

  test("ColumnRequestInput factory method creates ColumnFamilyRequestInput.") {
    val col = ColumnRequestInput(colFamily)
    assert(col.isInstanceOf[ColumnFamilyRequestInput])
    assert(colFamily === col.asInstanceOf[ColumnFamilyRequestInput].family)
  }

  test("Fields of a ColumnFamilyRequestOutput are the same as those it is constructed with.") {
    val col: ColumnFamilyRequestOutput = ColumnFamilyRequestOutput(colFamily, qualifierSelector)
    assert(colFamily === col.family)
    assert(qualifierSelector === col.qualifierSelector)
    assert(None === col.schemaSpec.schema)
  }

  test("ColumnFamilyRequestOutput factory method creates ColumnFamilyRequestOutput.") {
    val col = ColumnFamilyRequestOutput(colFamily, qualifierSelector, schema.get)

    assert(colFamily === col.family)
    assert(qualifierSelector === qualifierSelector)
    assert(schema === col.schemaSpec.schema)
  }

  test("Fields of a QualifiedColumnRequestInput are the same as those it is constructed with.") {
    val col: QualifiedColumnRequestInput = QualifiedColumnRequestInput(colFamily, colQualifier)
    assert(colFamily === col.family)
    assert(colQualifier === col.qualifier)
  }

  test("ColumnRequestInput factory method creates QualifiedColumnRequestInput.") {
    val col = QualifiedColumnRequestInput(colFamily, colQualifier)
    assert(col.isInstanceOf[QualifiedColumnRequestInput])
    assert(colFamily === col.asInstanceOf[QualifiedColumnRequestInput].family)
    assert(colQualifier === col.asInstanceOf[QualifiedColumnRequestInput].qualifier)
  }

  test("Fields of a QualifiedColumnRequestOutput are the same as those it is constructed with.") {
    val col: QualifiedColumnRequestOutput =
        QualifiedColumnRequestOutput(colFamily, colQualifier)

    assert(colFamily === col.family)
    assert(colQualifier === col.qualifier)
    assert(None === col.schemaSpec.schema)
  }

  test("QualifiedColumnRequestOutput factory method creates QualifiedColumnRequestOutput.") {
    val col = QualifiedColumnRequestOutput(colFamily, colQualifier, schema.get)
    assert(colQualifier === col.qualifier)
    assert(colFamily === col.family)
    assert(schema === col.schemaSpec.schema)
  }

  test("Two ColumnFamilys with the same parameters are equal and hash to the same value.") {
    val col1 = new ColumnFamilyRequestInput(colFamily)
    val col2 = new ColumnFamilyRequestInput(colFamily)

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("Two qualified columns with the same parameters are equal and hash to the same value.") {
    val col1 = new QualifiedColumnRequestInput(colFamily, colQualifier)
    val col2 = new QualifiedColumnRequestInput(colFamily, colQualifier)

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("maxVersions is the same as constructed with.") {
    assert(maxVersions == colWithOptions.maxVersions)
  }

  test("Default maxVersions is 1.") {
    assert(1 == QualifiedColumnRequestInput(colFamily, colQualifier).maxVersions)
  }

  test("Filter is the same as constructed with.") {
    assert(Some(filter) == colWithOptions.filter)
  }

  test("ColumnRequestInput with the same maxVersions & filter are equal and hash to the same "
      + "value.") {

    val col2: QualifiedColumnRequestInput = new QualifiedColumnRequestInput(
        colFamily, colQualifier,
        filter = Some(filter),
        maxVersions = maxVersions
    )
    assert(col2 === colWithOptions)
    assert(col2.hashCode() === colWithOptions.hashCode())
  }
}
