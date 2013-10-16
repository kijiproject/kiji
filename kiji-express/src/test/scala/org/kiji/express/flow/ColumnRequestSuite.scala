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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

@RunWith(classOf[JUnitRunner])
class ColumnRequestSuite extends FunSuite {
  def filter: KijiColumnFilter = new RegexQualifierColumnFilter(".*")
  def opts: ColumnRequestOptions = new ColumnRequestOptions(1, Some(filter))
  val colFamily = "myfamily"
  val colQualifier = "myqualifier"
  val qualifierSelector = "qualifierSym"

  test("Fields of a ColumnFamily (input) are the same as those it is constructed with.") {
    val col: ColumnFamily = new ColumnFamily(colFamily, None, opts)

    assert(colFamily === col.family)
    assert(opts === col.options)
  }

  test("Fields of a ColumnFamily (output) are the same as those it is constructed with.") {
    val col: ColumnFamily = new ColumnFamily(colFamily, Some(qualifierSelector))

    assert(colFamily === col.family)
    assert(qualifierSelector === col.qualifierSelector.get)
  }

  test("Fields of a QualifiedColumn are the same as those it is constructed with.") {
    val col: QualifiedColumn = new QualifiedColumn(colFamily, colQualifier, opts)

    assert(colFamily === col.family)
    assert(colQualifier === col.qualifier)
    assert(opts === col.options)
  }

  test("Two ColumnFamilys with the same parameters are equal and hash to the same value.") {
    val col1: ColumnFamily = new ColumnFamily(colFamily, None, opts)
    val col2: ColumnFamily = new ColumnFamily(colFamily, None, opts)

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("Two qualified columns with the same parameters are equal and hash to the same value.") {
    val col1: QualifiedColumn = new QualifiedColumn(colFamily, colQualifier, opts)
    val col2: QualifiedColumn = new QualifiedColumn(colFamily, colQualifier, opts)

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("A column must be serializable.") {
    // Serialize and deserialize using java ObjectInputStream and ObjectOutputStream.
    val col: QualifiedColumn =
        new QualifiedColumn(colFamily, colQualifier, opts)
    val bytesOut = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytesOut)
    out.writeObject(col)
    val serializedColumn = bytesOut.toByteArray()
    val bytesIn = new ByteArrayInputStream(serializedColumn)
    val in = new ObjectInputStream(bytesIn)
    val deserializedColumn = in.readObject()

    assert(col === deserializedColumn)
  }
}
