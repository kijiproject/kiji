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

package org.kiji.schema.shell.ddl

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import org.specs2.mutable._

import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.RowKeyFormat2
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.ddl.key._

class TestCreateTableCommand extends CommandTestCase {
  "CreateTableCommand" should {
    "create layouts <= max version we support" in {
      // create layout version is of lesser (-1) or equal (0) to max supported version.
      val ctcmd = new CreateTableCommand(env, "foo", Some("desc"), DefaultKeySpec, List(), Map())
      (CreateTableCommand.DDL_LAYOUT_VERSION.compareTo(ctcmd.MAX_LAYOUT_VERSION) must
          beLessThan(1))

      ok("Completed test")
    }

    "support a max version less than KijiSchema's max supported version" in {
      // If this check fails, then this tool needs to depend on a newer version of KijiSchema.

      // Defined as TableDDLCommand.MAX_LAYOUT_VERSION
      val ctcmd = new CreateTableCommand(env, "foo", Some("desc"), DefaultKeySpec, List(), Map())
      (ctcmd.MAX_LAYOUT_VERSION.compareTo(KijiTableLayout.getMaxSupportedLayoutVersion())
          must beLessThan(1))

      ok("Completed test")
    }

    "require 1+ locality groups" in {
      val ctcmd = new CreateTableCommand(env, "foo", Some("desc"), DefaultKeySpec, List(), Map())
      ctcmd.validateArguments() must throwA[DDLException]

      ok("Completed test")
    }

    "require non-empty name" in {
      val ctcmd = new CreateTableCommand(env, "", Some("desc"), RawFormattedKeySpec, List(), Map())
      ctcmd.validateArguments() must throwA[DDLException]

      ok("Completed test")
    }

    "create reasonable looking Avro records" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", None, new HashedFormattedKeySpec,
          List(locGroup), Map())

      ctcmd.validateArguments()
      val layout = ctcmd.getInitialLayout()
      ctcmd.updateLayout(layout)
      layout.getDescription() mustEqual ""
      layout.getName() mustEqual "foo"
      KijiTableLayout.getEncoding(layout.getKeysFormat()) mustEqual RowKeyEncoding.FORMATTED
      val locGroupAvroList = layout.getLocalityGroups()
      locGroupAvroList.size mustEqual 1
      val locGroupAvro = locGroupAvroList.head
      locGroupAvro.getName() mustEqual "default"

      // Check that this succeeds. MockKijiSystem will validate that enough
      // default values are populated.
      ctcmd.applyUpdate(layout.build())

      ok("Completed test")
    }

    "support hash prefix(2) as default row format" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", None, DefaultKeySpec, List(locGroup), Map())

      ctcmd.validateArguments()
      val layout = ctcmd.getInitialLayout()
      ctcmd.updateLayout(layout)
      KijiTableLayout.getEncoding(layout.getKeysFormat()) mustEqual RowKeyEncoding.FORMATTED
      layout.getKeysFormat().asInstanceOf[RowKeyFormat2].getSalt().getHashSize() mustEqual 2
      layout.getKeysFormat().asInstanceOf[RowKeyFormat2]
          .getSalt().getSuppressKeyMaterialization() mustEqual false
      layout.getKeysFormat().asInstanceOf[RowKeyFormat2].getComponents().size mustEqual 1
      layout.getKeysFormat().asInstanceOf[RowKeyFormat2].getComponents()(0).getName mustEqual "key"

      // Check that this succeeds. MockKijiSystem will validate that enough
      // default values are populated.
      ctcmd.applyUpdate(layout.build())

      ok("Completed test")
    }

    "support row format raw" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", None, RawFormattedKeySpec, List(locGroup),
          Map())

      ctcmd.validateArguments()
      val layout = ctcmd.getInitialLayout()
      ctcmd.updateLayout(layout)
      KijiTableLayout.getEncoding(layout.getKeysFormat()) mustEqual RowKeyEncoding.RAW

      // Check that this succeeds. MockKijiSystem will validate that enough
      // default values are populated.
      ctcmd.applyUpdate(layout.build())

      ok("Completed test")
    }

    "support row format hash prefix" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", None,
          new HashPrefixKeySpec(4), List(locGroup), Map())

      ctcmd.validateArguments()
      val layout = ctcmd.getInitialLayout()
      ctcmd.updateLayout(layout)
      KijiTableLayout.getEncoding(layout.getKeysFormat()) mustEqual RowKeyEncoding.FORMATTED
      layout.getKeysFormat().asInstanceOf[RowKeyFormat2].getSalt().getHashSize() mustEqual 4
      layout.getKeysFormat().asInstanceOf[RowKeyFormat2].getComponents().size mustEqual 1
      layout.getKeysFormat().asInstanceOf[RowKeyFormat2].getComponents()(0).getName mustEqual "key"

      // Check that this succeeds. MockKijiSystem will validate that enough
      // default values are populated.
      ctcmd.applyUpdate(layout.build())

      ok("Completed test")
    }

    "fail if hashprefix size is greater than 16" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", None,
          new HashPrefixKeySpec(20), List(locGroup), Map())

      ctcmd.validateArguments() must throwA[DDLException]

      ok("Completed test")
    }

    "fail if hashprefix size is less than 1" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", None,
          new HashPrefixKeySpec(0), List(locGroup), Map())

      ctcmd.validateArguments() must throwA[DDLException]

      val ctcmd2 = new CreateTableCommand(env, "foo", None,
          new HashPrefixKeySpec(-2), List(locGroup), Map())

      ctcmd.validateArguments() must throwA[DDLException]

      ok("Completed test")
    }

    "refuse to create tables that already exist" in {
      val locGroup = new LocalityGroupClause("default", None, List())
      val ctcmd = new CreateTableCommand(env, "foo", Some("desc"), DefaultKeySpec,
          List(locGroup), Map())

      ctcmd.validateArguments()
      val layout = ctcmd.getInitialLayout()
      ctcmd.updateLayout(layout)
      layout.getDescription() mustEqual "desc" // Check that Some(desc) works.

      ctcmd.applyUpdate(layout.build()) // This should succeed.
      ctcmd.validateArguments() must throwA[DDLException] // But now the table exists. This fails.

      ok("Completed test")
    }
  }
}
