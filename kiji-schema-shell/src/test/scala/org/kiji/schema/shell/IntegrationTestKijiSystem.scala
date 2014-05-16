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

package org.kiji.schema.shell

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.hbase.HBaseFactory
import org.kiji.schema.hbase.KijiManagedHBaseTableName
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.shell.ddl.TableProperties
import org.kiji.schema.shell.util.KijiTestHelpers
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.input.NullInputSource

/**
 * Tests that KijiSystem will connect to the correct Kiji instance and will actually create,
 * modify, and drop tables.
 */
class IntegrationTestKijiSystem
    extends SpecificationWithJUnit
    with TableProperties
    with KijiTestHelpers {
  "KijiSystem" should {
    "create a table correctly" in {
      val uri = getNewInstanceURI()
      installKiji(uri)
      val environment = env(uri)
      val parser = getParser(environment)
      val res = parser.parseAll(parser.statement, """
CREATE TABLE foo WITH DESCRIPTION 'some data'
ROW KEY FORMAT HASHED
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'basic information' (
    name "string" WITH DESCRIPTION 'The user\'s name',
    email "string",
    age "int"),
  MAP TYPE FAMILY integers COUNTER
);""")
      res.successful mustEqual true
      val env2 = res.get.exec()

      // Print the table description to the log for debugging purposes.
      val parser2 = getParser(env2)
      val res2 = parser2.parseAll(parser2.statement, "DESCRIBE foo;")
      res2.successful mustEqual true
      val env3 = res2.get.exec()

      // Programmatically test proper table creation.
      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout = env3.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout must beSome[KijiTableLayout]
      val layout = maybeLayout.get.getDesc
      val locGroups = layout.getLocalityGroups()
      locGroups.size mustEqual 1
      val defaultLocGroup = locGroups.head
      defaultLocGroup.getName().toString() mustEqual "default"
      defaultLocGroup.getFamilies().size mustEqual 2

      (defaultLocGroup.getFamilies().filter({grp => grp.getName().toString() == "integers"})
          .size mustEqual 1)
      val maybeInfo = defaultLocGroup.getFamilies().find({ grp =>
          grp.getName().toString() == "info" })
      maybeInfo must beSome[FamilyDesc]
      maybeInfo.get.getColumns().size mustEqual 3

      environment.kijiSystem.shutdown()

      ok("Completed test")
    }

    "create and drop a table" in {
      val uri = getNewInstanceURI()
      installKiji(uri)
      val environment = env(uri)
      val parser = getParser(environment)
      val res = parser.parseAll(parser.statement, """
CREATE TABLE foo WITH DESCRIPTION 'some data'
ROW KEY FORMAT HASHED
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'basic information' (
    name "string" WITH DESCRIPTION 'The user\'s name',
    email "string",
    age "int")
);""")
      res.successful mustEqual true
      val env2 = res.get.exec()

      // Verify that it's there.
      val maybeLayout = env2.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout must beSome[KijiTableLayout]

      // Drop the table.
      val parser2 = getParser(env2)
      val res2 = parser2.parseAll(parser2.statement, "DROP TABLE foo;")
      res2.successful mustEqual true
      val env3 = res2.get.exec()

      // Programmatically test that the table no longer exists.
      val maybeLayout2 = env3.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout2 must beNone

      environment.kijiSystem.shutdown()

      ok("Completed test")
    }

    "create and alter a table" in {
      val uri = getNewInstanceURI()
      installKiji(uri)
      val environment = env(uri)
      val parser = getParser(environment)
      val res = parser.parseAll(parser.statement, """
CREATE TABLE foo WITH DESCRIPTION 'some data'
ROW KEY FORMAT HASHED
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'basic information' (
    name "string" WITH DESCRIPTION 'The user\'s name',
    email "string",
    age "int"),
  MAP TYPE FAMILY integers COUNTER
);""")
      res.successful mustEqual true
      val env2 = res.get.exec()

      // Print the table description to the log for debugging purposes.
      val parser2 = getParser(env2)
      val res2 = parser2.parseAll(parser2.statement, "DESCRIBE foo;")
      res2.successful mustEqual true
      val env3 = res2.get.exec()

      // Programmatically test proper table creation.
      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout = env3.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout must beSome[KijiTableLayout]
      val layout = maybeLayout.get.getDesc
      val locGroups = layout.getLocalityGroups()
      locGroups.size mustEqual 1
      val defaultLocGroup = locGroups.head
      defaultLocGroup.getName().toString() mustEqual "default"
      defaultLocGroup.getFamilies().size mustEqual 2
      (defaultLocGroup.getFamilies().find({ fam => fam.getName().toString == "integers"})
          must beSome[FamilyDesc])
      (defaultLocGroup.getFamilies().find({ fam => fam.getName().toString == "info" })
          .get.getColumns.size mustEqual 3)

      // Now, drop the map-type family, and verify that this worked correctly.
      val parser3 = getParser(env3)
      val res3 = parser3.parseAll(parser3.statement, "ALTER TABLE foo DROP FAMILY integers;")
      res3.successful mustEqual true
      val env4 = res3.get.exec()

      println("Describing the table again, for debugging purposes.")
      val parser3a = getParser(env4)
      val res3a = parser3a.parseAll(parser3a.statement, "DESCRIBE foo;")
      res3a.successful mustEqual true
      val env4a = res3a.get.exec()

      // Programmatically inspect the modified layout, verify that
      // 'integers' is not present, but 'info' is.
      val maybeLayout2 = env4.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
      val defaultLocGroup2 = locGroups2.head
      defaultLocGroup2.getName().toString() mustEqual "default"
      defaultLocGroup2.getFamilies().size mustEqual 1
      defaultLocGroup2.getFamilies().head.getName().toString() mustEqual "info"

      // Now make a second edit to the same table. Make sure that the appropriate reference
      // id for the parent layout is being used.
      val parser4 = getParser(env4)
      val res4 = parser4.parseAll(parser4.statement, "ALTER TABLE foo SET DESCRIPTION = 'ohai';")
      res4.successful mustEqual true
      val env5 = res4.get.exec()

      // Inspect the modified layout. Verify the table description has been changed.
      val maybeLayout3 = env5.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      layout3.getDescription().toString mustEqual "ohai"

      environment.kijiSystem.shutdown()

      ok("Completed test")
    }

    "create a multi-region table" in {
      val uri = getNewInstanceURI()
      installKiji(uri)
      val environment = env(uri)
      val parser = getParser(environment)
      val res = parser.parseAll(parser.statement, """
CREATE TABLE foo WITH DESCRIPTION 'some data'
ROW KEY FORMAT HASHED
PROPERTIES ( NUMREGIONS = 10 )
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'basic information' (
    name "string" WITH DESCRIPTION 'The user\'s name',
    email "string",
    age "int")
);""")
      res.successful mustEqual true
      val env2 = res.get.exec()

      // Verify that it's there.
      val maybeLayout = env2.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout must beSome[KijiTableLayout]

      // And verify the region count is accurate.
      val hbaseTableName = KijiManagedHBaseTableName.getKijiTableName(
          uri.getInstance(), "foo").toBytes()
      val hbaseFactory = HBaseFactory.Provider.get()
      val hbaseAdmin = hbaseFactory.getHBaseAdminFactory(uri).create(HBaseConfiguration.create())
      hbaseAdmin.getTableRegions(hbaseTableName).size mustEqual 10
      hbaseAdmin.close()

      // Verify that we properly set the initial region count in the meta table.
      val regionCount: Option[String] = environment.kijiSystem.getMeta(uri, "foo",
         RegionCountMetaKey /* from TableProperties */)
      regionCount must beSome[String]
      regionCount.get mustEqual "10"

      environment.kijiSystem.shutdown()

      ok("Completed test")
    }

    "refuse to create a multi-region RAW table" in {
      val uri = getNewInstanceURI()
      installKiji(uri)
      val environment = env(uri)
      val parser = getParser(environment)
      val res = parser.parseAll(parser.statement, """
CREATE TABLE foo WITH DESCRIPTION 'some data'
ROW KEY FORMAT RAW
PROPERTIES ( NUMREGIONS = 10 )
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'basic information' (
    name "string" WITH DESCRIPTION 'The user\'s name',
    email "string",
    age "int")
);""")
      res.successful mustEqual true
      res.get.exec() must throwAn[Exception]

      // Verify that it's not there.
      val maybeLayout = environment.kijiSystem.getTableLayout(uri, "foo")
      maybeLayout must beNone

      environment.kijiSystem.shutdown()

      ok("Completed test")
    }

    "create, use, and drop an instance" in {
      val uri = getNewInstanceURI()
      val instanceName = uri.getInstance()
      val environment = env(uri)
      val parser = getParser(environment)

      println("Creating instance: " + uri.getInstance())
      val res = parser.parseAll(parser.statement, "CREATE INSTANCE '" + instanceName + "';")
      res.successful mustEqual true
      val env2 = res.get.exec()

      env2.kijiSystem.listInstances.contains(instanceName) mustEqual true

      // Print the instance list to the log for debugging purposes.
      println("Listing available instances...")
      val parser2 = getParser(env2)
      val res2 = parser2.parseAll(parser2.statement, "SHOW INSTANCES;")
      res2.successful mustEqual true
      val env3 = res2.get.exec()

      // Programmatically test proper table creation.
      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      println("Creating a table (tblfoo)...")
      val parser3 = getParser(env3)
      val res3 = parser3.parseAll(parser3.statement,
          "CREATE TABLE tblfoo WITH LOCALITY GROUP lg;")
      res3.successful mustEqual true
      val env4 = res3.get.exec()

      // Print the table list to the log for debugging purposes.
      println("Listing available tables")
      val parser4 = getParser(env4)
      val res4 = parser4.parseAll(parser4.statement, "SHOW TABLES;")
      res4.successful mustEqual true
      val env5 = res4.get.exec()

      // Create another instance; this implicitly switches to it.
      val alternateInstance = instanceName + "ALT"
      println("Creating alternate instance: " + alternateInstance)
      val parser5 = getParser(env5)
      val res5 = parser5.parseAll(parser5.statement,
          "CREATE INSTANCE '" + alternateInstance + "';")
      res5.successful mustEqual true
      val env6 = res5.get.exec()

      // Now remove the original instance.
      println("Dropping instance '" + instanceName + "'")
      val parser6 = getParser(env6)
      val res6 = parser6.parseAll(parser6.statement,
          "DROP INSTANCE '" + instanceName + "';")
      res5.successful mustEqual true
      val env7 = res6.get.exec()

      env7.kijiSystem.listInstances.contains(instanceName) mustEqual false

      println("Listing available instances one last time.")
      val parser7 = getParser(env7)
      val res7 = parser7.parseAll(parser7.statement, "SHOW INSTANCES;")
      res7.successful mustEqual true
      val env8 = res7.get.exec()

      environment.kijiSystem.shutdown()
      env8.kijiSystem.shutdown()

      ok("Completed test")
    }

    "use metatable entries correctly." in {
      // We want to set and retrieve metatable key/value pairs here.
      // We explicitly depend on the ability to set and retrieve these for tables that do
      // not (yet) exist.
      val uri = getNewInstanceURI()
      installKiji(uri)

      val environment = env(uri)

      environment.kijiSystem.getMeta(uri, "AnyTable", "AnyKey") must beNone
      environment.kijiSystem.setMeta(uri, "AnyTable", "AnyKey", "AWellDefinedValue")
      val ret: Option[String] = environment.kijiSystem.getMeta(uri, "AnyTable", "AnyKey")
      ret must beSome[String]
      ret.get mustEqual "AWellDefinedValue"

      environment.kijiSystem.shutdown()

      ok("Completed test")
    }
  }

  /**
   * Get an Environment instance.
   */
  def env(instanceURI: KijiURI): Environment = {
    new Environment(
      instanceURI,
      System.out,
      new KijiSystem,
      new NullInputSource(),
      List(),
      false)
  }
}
