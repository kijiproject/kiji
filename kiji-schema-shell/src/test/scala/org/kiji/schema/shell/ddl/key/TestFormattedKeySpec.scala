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

package org.kiji.schema.shell.ddl.key

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.specs2.mutable._

import org.kiji.schema.avro._
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.ddl.CommandTestCase

class TestFormattedKeySpec extends CommandTestCase {
  "FormattedKeySpec" should {

    "require nonempty params" in {
      val spec = new FormattedKeySpec(List())
      spec.validate() must throwA[DDLException]
    }

    "successfully create raw key spec" in {
      val spec = RawFormattedKeySpec
      spec.validate()
      val components = ListBuffer()
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.RAW)
          .setSalt(null)
          .setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "successfully create default key spec" in {
      val spec = DefaultKeySpec
      spec.validate()
      val components = ListBuffer(RowKeyComponent.newBuilder()
          .setName("key").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setSalt(HashSpec.newBuilder().setHashSize(2).build())
          .setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "successfully create hashed key spec" in {
      val spec = new HashedFormattedKeySpec
      spec.validate()
      val components = ListBuffer(RowKeyComponent.newBuilder()
          .setName("key").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setSalt(HashSpec.newBuilder().setSuppressKeyMaterialization(true).build())
          .setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "successfully create hash prefixed key spec" in {
      val spec = new HashPrefixKeySpec(10)
      spec.validate()
      val salt = HashSpec.newBuilder().setHashSize(10).build()
      val components = ListBuffer(RowKeyComponent.newBuilder()
          .setName("key").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setSalt(salt).setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "require hash prefix > 1" in {
      val spec = new HashPrefixKeySpec(0)
      spec.validate() must throwA[DDLException]
    }

    "be okay with a short hash prefix" in {
      val spec = new HashPrefixKeySpec(2)
      spec.validate()

      ok("Completed test")
    }

    "be okay with a long hash prefix" in {
      val spec = new HashPrefixKeySpec(16)
      spec.validate()

      ok("Completed test")
    }

    "require hash prefix <= 16" in {
      val spec = new HashPrefixKeySpec(17)
      spec.validate() must throwA[DDLException]
    }

    "require existing component in HASH THROUGH" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeyHashComponent("missing"),
          new FormattedKeyHashSize(12)
        ))
      ))

      spec.validate() must throwA[DDLException]
    }

    "require valid size" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeyHashComponent("key"),
          new FormattedKeyHashSize(-1)
        ))
      ))

      spec.validate() must throwA[DDLException]
    }

    "require valid size 2" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeyHashComponent("key"),
          new FormattedKeyHashSize(17)
        ))
      ))

      spec.validate() must throwA[DDLException]
    }

    "require at most one size element" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeyHashSize(2),
          new FormattedKeyHashSize(2)
        ))
      ))

      spec.validate() must throwA[DDLException]
    }

    "require at most one THROUGH element" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeyHashComponent("key"),
          new FormattedKeyHashComponent("key")
        ))
      ))

      spec.validate() must throwA[DDLException]
    }

    "require a component name at most once" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false),
        new KeyComponent("key", RowKeyElemType.STRING, mayBeNull=false)
      ))

      spec.validate() must throwA[DDLException]
    }

    "create a reasonable composite object" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=false),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=false)
      ))

      spec.validate()

      val salt = HashSpec.newBuilder().setHashSize(2).build()
      val components = ListBuffer(
          RowKeyComponent.newBuilder().setName("a").setType(ComponentType.STRING).build(),
          RowKeyComponent.newBuilder().setName("b").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setNullableStartIndex(2)
          .setSalt(salt).setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "support hash through both elems" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=false),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeyHashComponent("b")
        ))
      ))

      spec.validate()

      val salt = HashSpec.newBuilder().setHashSize(2).build()
      val components = ListBuffer(
          RowKeyComponent.newBuilder().setName("a").setType(ComponentType.STRING).build(),
          RowKeyComponent.newBuilder().setName("b").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setRangeScanStartIndex(2)
          .setNullableStartIndex(2)
          .setSalt(salt).setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "support explicit hash through all elems if key materialization is false" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=false),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeyHashComponent("b"),
          new FormattedKeySuppressFields()
        ))
      ))

      spec.validate()

      val salt = HashSpec.newBuilder().setHashSize(16).setSuppressKeyMaterialization(true).build()
      val components = ListBuffer(
          RowKeyComponent.newBuilder().setName("a").setType(ComponentType.STRING).build(),
          RowKeyComponent.newBuilder().setName("b").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setRangeScanStartIndex(2)
          .setNullableStartIndex(2)
          .setSalt(salt).setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "support implicit hash through all elems if key materialization is false" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=false),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeySuppressFields()
        ))
      ))

      spec.validate()

      val salt = HashSpec.newBuilder().setHashSize(16).setSuppressKeyMaterialization(true).build()
      val components = ListBuffer(
          RowKeyComponent.newBuilder().setName("a").setType(ComponentType.STRING).build(),
          RowKeyComponent.newBuilder().setName("b").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setRangeScanStartIndex(2) // should be 2, just like in the previous test case.
          .setNullableStartIndex(2)
          .setSalt(salt).setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "fail if key materialization is false but hash-thru isn't set to components.size" in {
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=false),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=false),
        new KeyHashParams(List(
          new FormattedKeySuppressFields(),
          new FormattedKeyHashComponent("a")
        ))
      ))

      spec.validate()
      spec.createFormattedKey() must throwA[DDLException]
    }

    "allow the first to be nullable with no consequence" in {
      // it's not actually nullable. the NOT NULL isn't necessary on the first elem, it's implied.
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=true),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=false)
      ))

      spec.validate()

      val salt = HashSpec.newBuilder().setHashSize(2).build()
      val components = ListBuffer(
          RowKeyComponent.newBuilder().setName("a").setType(ComponentType.STRING).build(),
          RowKeyComponent.newBuilder().setName("b").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setNullableStartIndex(2)
          .setSalt(salt).setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "let nullable index be one" in {
      // it's not actually nullable. the NOT NULL isn't necessary on the first elem, it's implied.
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=true),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=true)
      ))

      spec.validate()

      val salt = HashSpec.newBuilder().setHashSize(2).build()
      val components = ListBuffer(
          RowKeyComponent.newBuilder().setName("a").setType(ComponentType.STRING).build(),
          RowKeyComponent.newBuilder().setName("b").setType(ComponentType.STRING).build())
      val expected = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
          .setSalt(salt).setComponents(components).build()
      spec.createFormattedKey() mustEqual expected
    }

    "not allow non-null after null" in {
      // it's not actually nullable. the NOT NULL isn't necessary on the first elem, it's implied.
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=true),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=true),
        new KeyComponent("c", RowKeyElemType.STRING, mayBeNull=false)
      ))

      spec.validate() must throwA[DDLException]
    }

    "not allow non-null after null, noting the first component's nonnull doesn't matter" in {
      // it's not actually nullable. the NOT NULL isn't necessary on the first elem, it's implied.
      val spec = new FormattedKeySpec(List(
        new KeyComponent("a", RowKeyElemType.STRING, mayBeNull=false),
        new KeyComponent("b", RowKeyElemType.STRING, mayBeNull=true),
        new KeyComponent("c", RowKeyElemType.STRING, mayBeNull=false)
      ))

      spec.validate() must throwA[DDLException]
    }

  }
}
