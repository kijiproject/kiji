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

package org.kiji.mapreduce.shellext

import org.specs2.mutable._
import scala.collection.mutable.Map

import org.kiji.mapreduce.lib.bulkimport.CSVBulkImporter
import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.DDLParser
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.MockKijiSystem
import org.kiji.schema.shell.ddl.ColumnName
import org.kiji.schema.shell.ddl.UseModuleCommand
import org.kiji.schema.shell.input.NullInputSource
import org.kiji.schema.shell.spi.ParserPluginTestKit

/** Tests that the parser accepts the syntax we expect. */
class TestParser extends SpecificationWithJUnit {
  "The bulk import DDL parser" should {
    "Load the module" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "MODULE bulkimport;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[UseModuleCommand]
      res.get.exec() must not beNull
    }

    "Parse a simple raw import command" in {
      val parser = getParser()
      val env = loadBulkImportModule(parser)
      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, """
          LOAD DATA INFILE 'path/to/file' INTO TABLE 'foo' DIRECT
          USING 'com.example.FooImporter';
      """)

      res2.successful mustEqual true
      res2.get must beAnInstanceOf[BulkImportCommand]

      val cmd: BulkImportCommand = res2.get.asInstanceOf[BulkImportCommand]
      cmd.env mustEqual env
      cmd.className mustEqual "com.example.FooImporter"
      cmd.fileUri mustEqual "path/to/file"
      cmd.format mustEqual "text"
      cmd.tableName mustEqual "foo"
      cmd.via must beAnInstanceOf[LoadViaDirect]
      cmd.fieldMapping must beNone
      cmd.properties.size mustEqual 0
    }

    "Parse raw import with properties" in {
      val parser = getParser()
      val env = loadBulkImportModule(parser)
      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, """
          LOAD DATA INFILE 'path/to/file' INTO TABLE 'foo' DIRECT
          USING 'com.example.FooImporter'
          PROPERTIES ('a' = 'b', 'x' = 'y');
      """)

      res2.successful mustEqual true
      res2.get must beAnInstanceOf[BulkImportCommand]

      val cmd: BulkImportCommand = res2.get.asInstanceOf[BulkImportCommand]
      cmd.env mustEqual env
      cmd.className mustEqual "com.example.FooImporter"
      cmd.fileUri mustEqual "path/to/file"
      cmd.format mustEqual "text"
      cmd.tableName mustEqual "foo"
      cmd.via must beAnInstanceOf[LoadViaDirect]
      cmd.fieldMapping must beNone

      // This section is what's unique in this test case.
      val props: Map[String, String] = Map()
      props.put("a", "b")
      props.put("x", "y")
      cmd.properties.size mustEqual 2
      cmd.properties mustEqual props
    }

    "Parse raw import with format" in {
      val parser = getParser()
      val env = loadBulkImportModule(parser)
      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, """
          LOAD DATA INFILE 'path/to/file' FORMAT 'seq' INTO TABLE 'foo' DIRECT
          USING 'com.example.FooImporter';
      """)

      res2.successful mustEqual true
      res2.get must beAnInstanceOf[BulkImportCommand]

      val cmd: BulkImportCommand = res2.get.asInstanceOf[BulkImportCommand]
      cmd.env mustEqual env
      cmd.className mustEqual "com.example.FooImporter"
      cmd.fileUri mustEqual "path/to/file"
      cmd.format mustEqual "seq" // This is what's unique here.
      cmd.tableName mustEqual "foo"
      cmd.via must beAnInstanceOf[LoadViaDirect]
      cmd.fieldMapping must beNone
      cmd.properties.size mustEqual 0
    }

    "Parse raw import with indirect via clause" in {
      val parser = getParser()
      val env = loadBulkImportModule(parser)
      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, """
          LOAD DATA INFILE 'path/to/file' INTO TABLE 'foo' THROUGH PATH 'nowhere'
          USING 'com.example.FooImporter';
      """)

      res2.successful mustEqual true
      res2.get must beAnInstanceOf[BulkImportCommand]

      val cmd: BulkImportCommand = res2.get.asInstanceOf[BulkImportCommand]
      cmd.env mustEqual env
      cmd.className mustEqual "com.example.FooImporter"
      cmd.fileUri mustEqual "path/to/file"
      cmd.format mustEqual "text"
      cmd.tableName mustEqual "foo"
      cmd.via must beAnInstanceOf[LoadViaPath] // This is what's unique here.
      cmd.via.asInstanceOf[LoadViaPath].hdfsUri mustEqual "nowhere"
      cmd.fieldMapping must beNone
      cmd.properties.size mustEqual 0
    }

    "Parse implicit CSV import" in {
      val parser = getParser()
      val env = loadBulkImportModule(parser)
      val parser2 = new DDLParser(env)
      val res2 = parser2.parseAll(parser2.statement, """
          LOAD DATA INFILE 'path/to/file' INTO TABLE 'foo' THROUGH PATH 'nowhere'
          MAP FIELDS (a,b,c) AS (
            c => $TIMESTAMP,
            b => info:blort,
            DEFAULT FAMILY info,
            a => $ENTITY
          );
      """)

      res2.successful mustEqual true
      res2.get must beAnInstanceOf[BulkImportCommand]

      val cmd: BulkImportCommand = res2.get.asInstanceOf[BulkImportCommand]
      cmd.env mustEqual env

      // This class name is inferred
      cmd.className mustEqual classOf[CSVBulkImporter].getName()
      cmd.fileUri mustEqual "path/to/file"
      cmd.format mustEqual "text"
      cmd.tableName mustEqual "foo"
      cmd.via must beAnInstanceOf[LoadViaPath]
      cmd.via.asInstanceOf[LoadViaPath].hdfsUri mustEqual "nowhere"

      // This must be present here.
      cmd.fieldMapping must beSome[FieldMapping]
      val fields: FieldMapping = cmd.fieldMapping.get
      cmd.fieldMapping.get mustEqual new FieldMapping(env, Some(List("a","b","c")),
          List[FieldMappingElem](new TimestampFieldMapping("c"),
               new SingleFieldMapping("b", new ColumnName("info", "blort")),
               new DefaultFamilyMapping("info"),
               new EntityFieldMapping("a")))

      cmd.properties.size mustEqual 0
    }
  }

  /** @return a new parser for DDL commands in a new environment. */
  def getParser(): DDLParser = {
    val defaultURI = KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()
    new DDLParser(new Environment(
        defaultURI,
        System.out,
        new MockKijiSystem(),
        new NullInputSource))
  }

  /** @return an Environment with the bulk import module loaded. */
  def loadBulkImportModule(parser: DDLParser): Environment = {
    val res = parser.parseAll(parser.statement, "MODULE bulkimport;")
    if (!res.successful) {
      sys.error("Unsuccessful parse of 'MODULE bulkimport;'")
    }
    return res.get.exec() // Environment with bulk importer module loaded.
  }
}
