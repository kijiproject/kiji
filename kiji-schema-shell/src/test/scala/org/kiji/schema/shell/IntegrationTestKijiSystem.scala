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

import java.util.UUID
import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.kiji.schema.KijiInstaller
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.impl.DefaultHTableInterfaceFactory
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.shell.input.NullInputSource

/** Tests that KijiSystem will connect to the correct Kiji instance and will
  * actually create, modify, and drop tables.
  */
class IntegrationTestKijiSystem extends SpecificationWithJUnit {
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
    }
  }

  /**
   * @return the name of a unique Kiji instance (that doesn't yet exist).
   */
  def getNewInstanceURI(): KijiURI = {
    val instanceName = UUID.randomUUID().toString().replaceAll("-", "_");
    return KijiURI.newBuilder("kiji://.env/" + instanceName).build()
  }

  /**
   * Install a Kiji instance.
   */
  def installKiji(instanceURI: KijiURI): Unit = {
    KijiInstaller.get().install(instanceURI, HBaseConfiguration.create())
  }

  /**
   * Get an Environment instance.
   */
  def env(instanceURI: KijiURI): Environment = {
    new Environment(
        instanceURI,
        System.out,
        new KijiSystem,
        new NullInputSource())
  }

  /**
   * Get a new parser that's primed with the specified environment.
   */
  def getParser(environment: Environment): DDLParser = {
    new DDLParser(environment)
  }
}
