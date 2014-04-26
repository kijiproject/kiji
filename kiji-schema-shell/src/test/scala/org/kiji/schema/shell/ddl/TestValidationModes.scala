/**
 * (c) Copyright 2014 WibiData, Inc.
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

import org.kiji.schema.shell.util.KijiIntegrationTestHelpers
import org.specs2.mutable.SpecificationWithJUnit
import org.kiji.schema.shell.api.Client
import org.kiji.schema.avro.AvroValidationPolicy

/**
 * Tests for setting avro validation modes of columns.
 */
class TestValidationModes
    extends SpecificationWithJUnit
    with KijiIntegrationTestHelpers {

  "CreateTableCommand" should {
    "correctly set validation type on columns" in {
      val uri = getNewInstanceURI()
      val createTableCommand =
        """
          |CREATE TABLE 'table'
          |ROW KEY FORMAT (row STRING)
          |PROPERTIES (VALIDATION = STRICT)
          |WITH LOCALITY GROUP default (
          |  MAXVERSIONS = 1,
          |  TTL = FOREVER,
          |  INMEMORY = true,
          |  COMPRESSED WITH NONE,
          |  FAMILY id (
          |      indiv_id "string"
          |  )
          |);
        """.stripMargin

      val kijiSystem = getKijiSystem()
      val client = Client.newInstanceWithSystem(uri, kijiSystem)
      try {
        client.executeUpdate(createTableCommand)

        val layout = client.kijiSystem.getTableLayout(uri, "table")
        assert( layout.isDefined )
        assert( AvroValidationPolicy.STRICT == layout.get
            .getLocalityGroupMap
            .get("default")
            .getFamilyMap
            .get("id")
            .getColumnMap
            .get("indiv_id")
            .getDesc
            .getColumnSchema
            .getAvroValidationPolicy)
      } finally {
        client.close()
        kijiSystem.shutdown()
      }

      ok("Completed test")
    }
  }

  "AlterTableSetValidationModeCommand" should {
    "correctly change schema validation mode on columns" in {
      val uri = getNewInstanceURI()
      val createTableCommand =
        """
          |CREATE TABLE 'table'
          |ROW KEY FORMAT (row STRING)
          |PROPERTIES (VALIDATION = NONE)
          |WITH LOCALITY GROUP default (
          |  MAXVERSIONS = 1,
          |  TTL = FOREVER,
          |  INMEMORY = true,
          |  COMPRESSED WITH NONE,
          |  FAMILY id (
          |      indiv_id "string"
          |  )
          |);
        """.stripMargin

      val alterTableCommand =
        """
          |ALTER TABLE 'table'
          |SET VALIDATION = STRICT
          |FOR COLUMN id:indiv_id
        """.stripMargin

      val kijiSystem = getKijiSystem()
      val client = Client.newInstanceWithSystem(uri, kijiSystem)
      try {
        client.executeUpdate(createTableCommand)
        client.executeUpdate(alterTableCommand)

        val layout = client.kijiSystem.getTableLayout(uri, "table")
        assert( layout.isDefined )
        assert( AvroValidationPolicy.STRICT == layout.get
            .getLocalityGroupMap
            .get("default")
            .getFamilyMap
            .get("id")
            .getColumnMap
            .get("indiv_id")
            .getDesc
            .getColumnSchema
            .getAvroValidationPolicy)
      } finally {
        client.close()
        kijiSystem.shutdown()
      }

      ok("Completed test")
    }
  }

  "AlterTableSetValidationModeCommand" should {
    "correctly change schema validation mode on column families" in {
      val uri = getNewInstanceURI()
      val createTableCommand =
        """
          |CREATE TABLE 'table'
          |ROW KEY FORMAT (row STRING)
          |PROPERTIES (VALIDATION = NONE)
          |WITH LOCALITY GROUP default (
          |  MAXVERSIONS = 1,
          |  TTL = FOREVER,
          |  INMEMORY = true,
          |  COMPRESSED WITH NONE,
          |  FAMILY id (
          |      indiv_id "string",
          |      another_id "string"
          |  )
          |);
        """.stripMargin

      val alterTableCommand =
        """
          |ALTER TABLE 'table'
          |SET VALIDATION = STRICT
          |FOR FAMILY id
        """.stripMargin

      val kijiSystem = getKijiSystem()
      val client = Client.newInstanceWithSystem(uri, kijiSystem)
      try {
        client.executeUpdate(createTableCommand)
        client.executeUpdate(alterTableCommand)

        val layout = client.kijiSystem.getTableLayout(uri, "table")
        assert( layout.isDefined )
        assert( AvroValidationPolicy.STRICT == layout.get
            .getLocalityGroupMap
            .get("default")
            .getFamilyMap
            .get("id")
            .getColumnMap
            .get("indiv_id")
            .getDesc
            .getColumnSchema
            .getAvroValidationPolicy)
        assert( AvroValidationPolicy.STRICT == layout.get
            .getLocalityGroupMap
            .get("default")
            .getFamilyMap
            .get("id")
            .getColumnMap
            .get("another_id")
            .getDesc
            .getColumnSchema
            .getAvroValidationPolicy)
      } finally {
        client.close()
        kijiSystem.shutdown()
      }

      ok("Completed test")
    }
  }

  "AlterTableSetValidationModeCommand" should {
    "correctly change schema validation mode on map type column families" in {
      val uri = getNewInstanceURI()
      val createTableCommand =
        """
          |CREATE TABLE 'table'
          |ROW KEY FORMAT (row STRING)
          |PROPERTIES (VALIDATION = NONE)
          |WITH LOCALITY GROUP default (
          |  MAXVERSIONS = 1,
          |  TTL = FOREVER,
          |  INMEMORY = true,
          |  COMPRESSED WITH NONE,
          |  MAP TYPE FAMILY id
          |);
        """.stripMargin

      val alterTableCommand =
        """
          |ALTER TABLE 'table'
          |SET VALIDATION = STRICT
          |FOR FAMILY id
        """.stripMargin

      val kijiSystem = getKijiSystem()
      val client = Client.newInstanceWithSystem(uri, kijiSystem)
      try {
        client.executeUpdate(createTableCommand)
        client.executeUpdate(alterTableCommand)

        val layout = client.kijiSystem.getTableLayout(uri, "table")
        assert( layout.isDefined )
        assert( AvroValidationPolicy.STRICT == layout.get
            .getLocalityGroupMap
            .get("default")
            .getFamilyMap
            .get("id")
            .getDesc
            .getMapSchema
            .getAvroValidationPolicy)
      } finally {
        client.close()
        kijiSystem.shutdown()
      }

      ok("Completed test")
    }
  }

  "AlterTableSetValidationModeCommand" should {
    "correctly disable schema validation on a single column" in {
      val uri = getNewInstanceURI()
      val createTableCommand =
        """
          |CREATE TABLE 'table'
          |ROW KEY FORMAT (row STRING)
          |PROPERTIES (VALIDATION = STRICT)
          |WITH LOCALITY GROUP default (
          |  MAXVERSIONS = 1,
          |  TTL = FOREVER,
          |  INMEMORY = true,
          |  COMPRESSED WITH NONE,
          |  FAMILY id (
          |      indiv_id "string",
          |      other_id "string"
          |  )
          |);
        """.stripMargin

      val alterTableCommand =
        """
          |ALTER TABLE 'table'
          |SET VALIDATION = NONE
          |FOR COLUMN id:indiv_id
        """.stripMargin

      val kijiSystem = getKijiSystem()
      val client = Client.newInstanceWithSystem(uri, kijiSystem)
      try {
        client.executeUpdate(createTableCommand)
        client.executeUpdate(alterTableCommand)

        val layout = client.kijiSystem.getTableLayout(uri, "table")
        assert( layout.isDefined )
        assert( AvroValidationPolicy.STRICT == layout.get
            .getLocalityGroupMap
            .get("default")
            .getFamilyMap
            .get("id")
            .getColumnMap
            .get("other_id")
            .getDesc
            .getColumnSchema
            .getAvroValidationPolicy)
        assert( AvroValidationPolicy.NONE == layout.get
            .getLocalityGroupMap
            .get("default")
            .getFamilyMap
            .get("id")
            .getColumnMap
            .get("indiv_id")
            .getDesc
            .getColumnSchema
            .getAvroValidationPolicy)
      } finally {
        client.close()
        kijiSystem.shutdown()
      }

      ok("Completed test")
    }
  }
}
