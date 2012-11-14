/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.schema.shell

import org.specs2.mutable._
import org.kiji.schema.KijiConfiguration
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.RowKeyFormat
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.VersionInfo
import java.util.ArrayList

import org.kiji.schema.shell.input.NullInputSource

class TestMockKijiSystem extends SpecificationWithJUnit {
  "MockKijiSystem" should {
    "include three instances" in {
      val instances = new MockKijiSystem().listInstances()
      instances.size mustEqual 3
      instances.contains(KijiConfiguration.DEFAULT_INSTANCE_NAME) mustEqual true
      instances.contains("foo") mustEqual true
      instances.contains("a-missing-instance") mustEqual false
    }

    "allow create table" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(VersionInfo.getClientDataVersion())

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = new KijiTableLayout(avro, null)
      val sys = new MockKijiSystem
      sys.createTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", layout)
      (sys.getTableNamesDescriptions(KijiConfiguration.DEFAULT_INSTANCE_NAME)
          mustEqual List(("t", "desc")).toArray)
    }

    "support the Environment.containsTable operation" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(VersionInfo.getClientDataVersion())

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = new KijiTableLayout(avro, null)
      val sys = new MockKijiSystem
      sys.createTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", layout)

      new Environment(KijiConfiguration.DEFAULT_INSTANCE_NAME, Console.out,
        sys, new NullInputSource).containsTable("t") mustEqual true
    }

    "allow drop table" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(VersionInfo.getClientDataVersion())

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = new KijiTableLayout(avro, null)
      val sys = new MockKijiSystem
      sys.createTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", layout)
      (sys.getTableNamesDescriptions(KijiConfiguration.DEFAULT_INSTANCE_NAME)
          mustEqual List(("t", "desc")).toArray)
      sys.dropTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t")
      (sys.getTableNamesDescriptions(KijiConfiguration.DEFAULT_INSTANCE_NAME)
          mustEqual List[(String, String)]().toArray)
    }

    "disallow create table twice on the same name" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(VersionInfo.getClientDataVersion())
      val sys = new MockKijiSystem

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = new KijiTableLayout(avro, null)
      sys.createTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", layout)
      (sys.createTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", layout)
          must throwA[RuntimeException])
    }

    "disallow drop table on missing table" in {
      val sys = new MockKijiSystem
      sys.dropTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t") must throwA[RuntimeException]
    }

    "disallow apply layout on missing table" in {
      val sys = new MockKijiSystem
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(VersionInfo.getClientDataVersion())
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      avro.setName("t")
      avro.setDescription("meep")
      // Verify that this is a valid layout
      new KijiTableLayout(avro, null)
      // .. but you can't apply it to a missing table.
      (sys.applyLayout(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", avro)
          must throwA[RuntimeException])
    }

    "createTable should fail on malformed input records" in {
      val sys = new MockKijiSystem
      val avro: TableLayoutDesc = new TableLayoutDesc // Missing the localityGroups list, etc.
      new KijiTableLayout(avro, null) must throwA[RuntimeException]
    }

    "update layout with applyLayout" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(VersionInfo.getClientDataVersion())
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      avro.setName("t")
      avro.setDescription("desc1")
      val layout: KijiTableLayout = new KijiTableLayout(avro, null)
      val sys = new MockKijiSystem

      sys.createTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", layout)
      (sys.getTableNamesDescriptions(KijiConfiguration.DEFAULT_INSTANCE_NAME)
          mustEqual List(("t", "desc1")).toArray)

      val avro2: TableLayoutDesc = new TableLayoutDesc
      avro2.setLocalityGroups(new ArrayList())
      avro2.setVersion(VersionInfo.getClientDataVersion())
      avro2.setName("t")
      avro2.setDescription("desc2")
      avro2.setKeysFormat(rowKeyFormat)
      sys.applyLayout(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", avro2)
      (sys.getTableNamesDescriptions(KijiConfiguration.DEFAULT_INSTANCE_NAME)
          mustEqual List(("t", "desc2")).toArray)
    }

    "getTableLayout() should deep copy Avro records given to client" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(VersionInfo.getClientDataVersion())
      avro.setName("t")
      avro.setDescription("desc1")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout: KijiTableLayout = new KijiTableLayout(avro, null)
      val sys = new MockKijiSystem

      sys.createTable(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t", layout)
      (sys.getTableNamesDescriptions(KijiConfiguration.DEFAULT_INSTANCE_NAME)
          mustEqual List(("t", "desc1")).toArray)

      val maybeLayout2 = sys.getTableLayout(KijiConfiguration.DEFAULT_INSTANCE_NAME, "t")
      maybeLayout2 must beSome[KijiTableLayout]

      val layout2 = maybeLayout2 match {
        case Some(layout) => layout
        case None => throw new RuntimeException("Missing!")
      }

      layout2.getDesc().setDescription("desc2") // Prove that this updates a copy...

      // By verifying that the MockKijiSystem returns the original description.
      (sys.getTableNamesDescriptions(KijiConfiguration.DEFAULT_INSTANCE_NAME)
          mustEqual List(("t", "desc1")).toArray)
    }
  }
}
