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

package org.kiji.schema.shell.api

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.kiji.schema.KijiInstaller
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.KijiSystem
import org.kiji.schema.shell.input.NullInputSource

/** Tests that the api.Client interface does the right thing. */
class TestClient extends SpecificationWithJUnit {
  "The Client API" should {
    "create a table correctly" in {
      val uri = getNewInstanceURI()
      installKiji(uri)
      val client = Client.newInstance(uri)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  MAXVERSIONS = INFINITY,
          |  TTL = FOREVER,
          |  INMEMORY = false,
          |  COMPRESSED WITH GZIP,
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    name "string" WITH DESCRIPTION 'The user\'s name',
          |    email "string",
          |    age "int"),
          |  MAP TYPE FAMILY integers COUNTER
          |);""".stripMargin('|'))

      // Programmatically test proper table creation.
      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout = env(uri).kijiSystem.getTableLayout(uri, "foo")
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

      client.close()
    }
  }

  private var mNextInstanceId = 0;

  /**
   * @return the name of a unique Kiji instance (that doesn't yet exist).
   */
  def getNewInstanceURI(): KijiURI = {
    val id = mNextInstanceId;
    mNextInstanceId += 1;
    return KijiURI.newBuilder("kiji://.fake." + id + "/default").build()
  }

  /**
   * Install a Kiji instance.
   */
  def installKiji(instanceURI: KijiURI): Unit = {
    KijiInstaller.get().install(instanceURI, HBaseConfiguration.create())
  }

  private def env(uri: KijiURI) = {
    new Environment(
        uri,
        System.out,
        KijiSystem,
        new NullInputSource())
  }
}
