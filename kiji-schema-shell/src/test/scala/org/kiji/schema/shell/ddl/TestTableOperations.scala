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
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import org.specs2.mutable._

import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.avro._
import org.kiji.schema.layout.KijiTableLayout

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.DDLParser
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.InputProcessor
import org.kiji.schema.shell.input.StringInputSource

class TestTableOperations extends CommandTestCase {
  "Various table operations" should {
    "create a table" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
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
          |  MAP TYPE FAMILY integers COUNTER WITH DESCRIPTION 'metric tracking data'
          |);""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 16
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual true
      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
      val defaultLocGroup2 = locGroups2.head
      defaultLocGroup2.getName().toString() mustEqual "default"
      defaultLocGroup2.getFamilies().size mustEqual 2
      (defaultLocGroup2.getFamilies().filter({ grp => grp.getName().toString() == "integers" })
          .size mustEqual 1)
      val maybeInfo = defaultLocGroup2.getFamilies().find({ grp =>
          grp.getName().toString() == "info" })
      maybeInfo must beSome[FamilyDesc]
      maybeInfo.get.getColumns().size mustEqual 3
    }

    "Use the table MEMSTORE FLUSH SIZE property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |PROPERTIES (
        |  MEMSTORE FLUSH SIZE = 5000
        |)
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val memstoreFlushSize = layout2.getMemstoreFlushsize
      memstoreFlushSize mustEqual 5000
    }

    "Use the table MAX FILE SIZE property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |PROPERTIES (
        |  MAX FILE SIZE = 5000
        |)
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val maxFileSize = layout2.getMaxFilesize
      maxFileSize mustEqual 5000
    }

    "Use the table MAX FILE SIZE property with an invalid value" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |PROPERTIES (
        |  MAX FILE SIZE = -50
        |)
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      res.get.exec() must throwAn[Exception]
    }

    "Use the locality group BLOOM FILTER property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  BLOOM FILTER = ROW,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
      val defaultLocGroup2 = locGroups2.head
      defaultLocGroup2.getName().toString() mustEqual "default"
      defaultLocGroup2.getFamilies().size mustEqual 1
      defaultLocGroup2.getBloomType() mustEqual BloomType.ROW
    }

    "Use the locality group BLOCK SIZE property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  BLOCK SIZE = 100,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
      val defaultLocGroup2 = locGroups2.head
      defaultLocGroup2.getName().toString() mustEqual "default"
      defaultLocGroup2.getFamilies().size mustEqual 1
      defaultLocGroup2.getBlockSize() mustEqual 100
    }

    "Use the locality group BLOCK SIZE = NULL property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  BLOCK SIZE = nULL,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
      val defaultLocGroup2 = locGroups2.head
      defaultLocGroup2.getName().toString() mustEqual "default"
      defaultLocGroup2.getFamilies().size mustEqual 1
      defaultLocGroup2.getBlockSize() must beNull
    }

    "Use the locality group BLOCK SIZE property incorrectly" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  BLOCK SIZE = 0,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      res.get.exec() must throwAn[Exception]
    }

    "Set and alter the table MEMSTORE FLUSH SIZE property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |PROPERTIES (
        |  MEMSTORE FLUSH SIZE = 5000
        |)
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val memstoreFlushSize2 = layout2.getMemstoreFlushsize
      memstoreFlushSize2 mustEqual 5000

      val res2 = parser.parseAll(parser.statement, """
        |ALTER TABLE foo SET MEMSTORE FLUSH SIZE = 10000;""".stripMargin);
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableSetPropertyCommand]
      val env3 = res2.get.exec()

      val maybeLayout3 = env3.kijiSystem.getTableLayout(defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val memstoreFlushSize3 = layout3.getMemstoreFlushsize
      memstoreFlushSize3 mustEqual 10000
    }

    "Set and alter the table MAX FILE SIZE property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
        |CREATE TABLE foo WITH DESCRIPTION 'some data'
        |ROW KEY FORMAT HASHED
        |PROPERTIES (
        |  MAX FILE SIZE = 5000
        |)
        |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
        |  MAXVERSIONS = INFINITY,
        |  FAMILY info WITH DESCRIPTION 'basic information' (
        |    name "string" WITH DESCRIPTION 'The user\'s name'
        |  ));""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val maxFileSize2 = layout2.getMaxFilesize
      maxFileSize2 mustEqual 5000

      val res2 = parser.parseAll(parser.statement, """
        |ALTER TABLE foo SET MAX FILE SIZE = 10000;""".stripMargin);
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableSetPropertyCommand]
      val env3 = res2.get.exec()

      val maybeLayout3 = env3.kijiSystem.getTableLayout(defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val maxFileSize3 = layout3.getMaxFilesize
      maxFileSize3 mustEqual 10000
    }

    "create a table and add a column" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
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
          |    age "int")
          |);""".stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
      val defaultLocGroup2 = locGroups2.head
      defaultLocGroup2.getName().toString() mustEqual "default"
      defaultLocGroup2.getFamilies().size mustEqual 1
      defaultLocGroup2.getFamilies().head.getColumns().size mustEqual 3

      // Add a column.
      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo ADD COLUMN info:meep "string" WITH DESCRIPTION 'beep beep!';
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableAddColumnCommand]
      val env3 = res2.get.exec()

      // Check that the new column is here.
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      locGroups3.size mustEqual 1
      val defaultLocGroup3 = locGroups3.head
      defaultLocGroup3.getFamilies().size mustEqual 1
      defaultLocGroup3.getFamilies().head.getColumns().size mustEqual 4
      var meepExists = false
      defaultLocGroup3.getFamilies().head.getColumns().foreach { col =>
        if (col.getName().toString().equals("meep")) {
          meepExists = true
          getSchemaTextFor(env3, col.getColumnSchema().getDefaultReader()) mustEqual "\"string\""
        }
      }
      meepExists mustEqual true
    }

    "create a table and rename a column" in {
      val env2 = createBasicTable()

      // Add a column.
      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo RENAME COLUMN info:email AS info:mail;
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableRenameColumnCommand]
      val env3 = res2.get.exec()

      // Check that the new column is here.
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      locGroups3.size mustEqual 1
      val defaultLocGroup3 = locGroups3.head
      defaultLocGroup3.getFamilies().size mustEqual 1
      var mailExists = false
      var emailExists = false
      defaultLocGroup3.getFamilies().head.getColumns().foreach { col =>
        if (col.getName().toString().equals("mail")) {
          mailExists = true
          col.getColumnSchema().getValue() must beNull
          getSchemaTextFor(env3, col.getColumnSchema().getDefaultReader()) mustEqual "\"string\""
        } else if (col.getName().toString().equals("email")) {
          emailExists = true
        }
      }
      mailExists mustEqual true
      emailExists mustEqual false
    }

    "create a table and drop a column" in {
      val env2 = createBasicTable()

      // Drop a column.
      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo DROP COLUMN info:email;
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableDropColumnCommand]
      val env3 = res2.get.exec()

      // Check that the new column is here.
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      val defaultLocGroup3 = locGroups3.head
      var emailExists = false
      defaultLocGroup3.getFamilies().head.getColumns().foreach { col =>
        if (col.getName().toString().equals("email")) {
          emailExists = true
        }
      }
      emailExists mustEqual false
    }

    "verify inputProcessor returns the right environment" in {
      val env2 = createBasicTable()

      val env3 = env2.withInputSource(new StringInputSource("""
          |CREATE TABLE t
          |WITH DESCRIPTION 'quit'
          |WITH LOCALITY GROUP lg;
      """.stripMargin))
      val inputProcessor = new InputProcessor
      val buf = new StringBuilder
      val resEnv = inputProcessor.processUserInput(buf, env3)
      resEnv.containsTable("t") mustEqual true

    }

    // TODO(DDLSHELL-60) The following test is currently disabled because the DDL parser
    //     doesn't support multi-line single-quoted strings.
    /*
    "parse statements with embedded quit line" in {
      val env2 = createBasicTable()

      val env3 = env2.withInputSource(new StringInputSource("""
          |CREATE TABLE t
          |WITH DESCRIPTION '
          |quit
          |'
          |WITH LOCALITY GROUP lg;
      """.stripMargin))
      val inputProcessor = new InputProcessor
      val buf = new StringBuilder
      val resEnv = inputProcessor.processUserInput(buf, env3)
      resEnv.containsTable("t") mustEqual true
    }
    */

    "parse statements over many lines" in {
      val env2 = createBasicTable()

      val env3 = env2.withInputSource(new StringInputSource("""
          |CREATE
          |TABLE
          |t
          |WITH
          |DESCRIPTION
          |'d'
          |WITH
          |LOCALITY GROUP lg;
      """.stripMargin))
      val inputProcessor = new InputProcessor
      val buf = new StringBuilder
      val resEnv = inputProcessor.processUserInput(buf, env3)
      resEnv.containsTable("t") mustEqual true
    }

    "create a table and drop a locality group" in {
      val env2 = createBasicTable()

      // Drop a locality group.
      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          ALTER TABLE foo DROP LOCALITY GROUP default;
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableDropLocalityGroupCommand]
      val env3 = res2.get.exec()

      // Check that we have 0 locality groups.
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      locGroups3.size mustEqual 0
    }

    "create a table and rename a locality group" in {
      val env2 = createBasicTable()

      // Rename locality group.
      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo RENAME LOCALITY GROUP default def;
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableRenameLocalityGroupCommand]
      val env3 = res2.get.exec()

      // Check that the locality group has the new name
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      locGroups3.size mustEqual 1
      locGroups3.head.getName().toString() mustEqual "def"
    }

    "create a table and update a family description" in {
      val env2 = createBasicTable()

      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo SET DESCRIPTION = 'testing!' FOR FAMILY info;
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableDescForFamilyCommand]
      val env3 = res2.get.exec()

      // Check that the family has a new description.
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      (layout3.getLocalityGroups().head.getFamilies().find({ fam =>
        fam.getName().toString == "info" }).get.getDescription().toString
        mustEqual "testing!")
    }

    "create a table and update the table description" in {
      val env2 = createBasicTable()

      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo SET DESCRIPTION = 'testing!';
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableDescCommand]
      val env3 = res2.get.exec()

      // Check that the table has an updated description.
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      maybeLayout3.get.getDesc().getDescription().toString() mustEqual "testing!"
    }

    "create a table and update the locality group description" in {
      val env2 = createBasicTable()

      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo SET DESCRIPTION = 'testing!' FOR LOCALITY GROUP 'default';
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterTableDescForLocalityGroupCommand]
      val env3 = res2.get.exec()

      // Check that the locality group has the new description
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      locGroups3.size mustEqual 1
      locGroups3.head.getName().toString() mustEqual "default"
      locGroups3.head.getDescription().toString() mustEqual "testing!"
    }

    "create a table and update a locality group property" in {
      val env2 = createBasicTable()

      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo SET INMEMORY = true FOR LOCALITY GROUP 'default';
      """.stripMargin)
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[AlterLocalityGroupPropertyCommand]
      val env3 = res2.get.exec()

      // Check that the locality group has the new property
      val maybeLayout3 = env3.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      locGroups3.size mustEqual 1
      locGroups3.head.getName().toString() mustEqual "default"
      locGroups3.head.getInMemory() mustEqual true
    }

    "alter a map type family schema" in {
      val env2 = createBasicTable()

      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          ALTER TABLE foo ADD MAP TYPE FAMILY ints "int" TO LOCALITY GROUP default;
      """.stripMargin)
      res2.successful mustEqual true
      val env3 = res2.get.exec()

      // Check that the new family exists.
      val maybeLayout3 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc
      val locGroups3 = layout3.getLocalityGroups()
      locGroups3.size mustEqual 1
      locGroups3.head.getFamilies().size mustEqual 2
      val maybeMapFamily = locGroups3.head.getFamilies().find({ fam =>
        fam.getName().toString == "ints"})
      maybeMapFamily must beSome[FamilyDesc]
      val mapFamily = maybeMapFamily.get
      mapFamily.getName().toString mustEqual "ints"
      getSchemaTextFor(env3, mapFamily.getMapSchema().getDefaultReader()) mustEqual "\"int\""

      // Set the new family's schema to "string".
      val parser3 = new DDLParser(env3)
      val res3 = parser3.parseAll(parser3.statement, """
          |ALTER TABLE foo SET SCHEMA = "string" FOR MAP TYPE FAMILY ints;
      """.stripMargin)
      res3.successful mustEqual true
      val env4 = res3.get.exec()
      val maybeLayout4 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout4 must beSome[KijiTableLayout]
      val layout4 = maybeLayout4.get.getDesc
      val locGroups4 = layout4.getLocalityGroups()
      val maybeMapFamily2 = locGroups4.head.getFamilies().find({ fam =>
        fam.getName().toString == "ints"})
      maybeMapFamily2 must beSome[FamilyDesc]
      val mapFamily2 = maybeMapFamily2.get

      mapFamily2.getName().toString mustEqual "ints"
      getSchemaTextFor(env4, mapFamily2.getMapSchema().getDefaultReader()) mustEqual "\"string\""
    }

    "alter a column schema" in {
      val env2 = createBasicTable()

      val parser2 = new DDLParser(env2)
      val res2 = parser2.parseAll(parser2.statement, """
          |ALTER TABLE foo SET SCHEMA = "int" FOR COLUMN info:email;
      """.stripMargin)
      res2.successful mustEqual true
      val env3 = res2.get.exec()

      // Check that the new family exists.
      val maybeLayout2 = env3.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
      locGroups2.head.getFamilies().head.getName().toString() mustEqual "info"
      locGroups2.head.getFamilies().head.getColumns().foreach { col =>
        if (col.getName().toString().equals("email")) {
          getSchemaTextFor(env3, col.getColumnSchema().getDefaultReader()) mustEqual "\"int\""
        }
      }

      ok("Completed test")
    }

    "dump table ddl correctly" in {
      val env2 = createBasicTable()
      val baos = new ByteArrayOutputStream()
      val printStream = new PrintStream(baos)
      val parser = new DDLParser(env2.withPrinter(printStream))
      val res2 = parser.parseAll(parser.statement, "DUMP DDL FOR TABLE foo;")
      res2.successful mustEqual true
      val env3 = res2.get.exec()

      // Retrieve the output DDL string from the environment.
      printStream.close()
      val generatedDdl = baos.toString()
      System.out.println("Got output ddl: [" + generatedDdl + "]")

      // Also record the current table layout.
      val maybeLayout = env3.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout must beSome[KijiTableLayout]
      val layout = maybeLayout.get.getDesc

      // Drop the table.
      val parser2 = new DDLParser(env3.withPrinter(System.out))
      val res3 = parser2.parseAll(parser2.statement, "DROP TABLE foo;")
      res3.successful mustEqual true
      val env4 = res3.get.exec()

      // Verify that the table is dropped.
      val maybeLayout2 = env4.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beNone

      // Recreate it using the auto-generated DDL.
      val ddlInputSource = new StringInputSource(generatedDdl)
      new InputProcessor().processUserInput(new StringBuilder(),
          env4.withInputSource(ddlInputSource))

      val maybeLayout3 = env4.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout3 must beSome[KijiTableLayout]
      val layout3 = maybeLayout3.get.getDesc

      // Check that the table layout is exactly the same as the original layout.
      System.out.println("layout rkf: " + layout.getKeysFormat().toString())
      System.out.println("layout3 rkf: " + layout3.getKeysFormat().toString())

      // The only place there will be differences, is in the layout id, which is governed
      // internally. Just null these fields.
      layout.setLayoutId(null)
      layout.setReferenceLayout(null)

      layout3.setLayoutId(null)
      layout3.setReferenceLayout(null)

      layout mustEqual layout3
    }

    "create a table with a formatted key" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT (a STRING, b INT)
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage';
      """.stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 2
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual false
      rowKey.getComponents().size mustEqual 2
      rowKey.getComponents()(0).getName mustEqual "a"
      rowKey.getComponents()(0).getType mustEqual ComponentType.STRING
      rowKey.getComponents()(1).getName mustEqual "b"
      rowKey.getComponents()(1).getType mustEqual ComponentType.INTEGER
      rowKey.getRangeScanStartIndex mustEqual 1
      rowKey.getNullableStartIndex mustEqual 1

      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
    }

    "create a table with a non-null formatted key" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT (a STRING, b INT NOT NULL)
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage';
      """.stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 2
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual false
      rowKey.getComponents().size mustEqual 2
      rowKey.getComponents()(0).getName mustEqual "a"
      rowKey.getComponents()(0).getType mustEqual ComponentType.STRING
      rowKey.getComponents()(1).getName mustEqual "b"
      rowKey.getComponents()(1).getType mustEqual ComponentType.INTEGER
      rowKey.getRangeScanStartIndex mustEqual 1
      rowKey.getNullableStartIndex mustEqual 2 // different

      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
    }

    "create a table with a composite hash of size 4" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT (a STRING, b INT, HASH(THROUGH b, SIZE=4))
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage';
      """.stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 4 // different
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual false
      rowKey.getComponents().size mustEqual 2
      rowKey.getComponents()(0).getName mustEqual "a"
      rowKey.getComponents()(0).getType mustEqual ComponentType.STRING
      rowKey.getComponents()(1).getName mustEqual "b"
      rowKey.getComponents()(1).getType mustEqual ComponentType.INTEGER
      rowKey.getRangeScanStartIndex mustEqual 2 // different
      rowKey.getNullableStartIndex mustEqual 1

      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
    }

    "create a table with a fully-hashed composite key" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT (a STRING, b INT, HASH(THROUGH b, SUPPRESS FIELDS))
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage';
      """.stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 16 // set through SUPPRESS FIELDS
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual true // different
      rowKey.getComponents().size mustEqual 2
      rowKey.getComponents()(0).getName mustEqual "a"
      rowKey.getComponents()(0).getType mustEqual ComponentType.STRING
      rowKey.getComponents()(1).getName mustEqual "b"
      rowKey.getComponents()(1).getType mustEqual ComponentType.INTEGER
      rowKey.getRangeScanStartIndex mustEqual 2 // different
      rowKey.getNullableStartIndex mustEqual 1

      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
    }

    "create a table with a fully-hashed composite key with overrode size" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT (a STRING, b INT, HASH(THROUGH b, SUPPRESS FIELDS, SIZE=10))
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage';
      """.stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 10 // the important setting to check
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual true // different
      rowKey.getComponents().size mustEqual 2
      rowKey.getComponents()(0).getName mustEqual "a"
      rowKey.getComponents()(0).getType mustEqual ComponentType.STRING
      rowKey.getComponents()(1).getName mustEqual "b"
      rowKey.getComponents()(1).getType mustEqual ComponentType.INTEGER
      rowKey.getRangeScanStartIndex mustEqual 2 // different
      rowKey.getNullableStartIndex mustEqual 1

      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
    }

    "create a table with an implicitly STRING key" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT (a, b INT)
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage';
      """.stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 2
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual false
      rowKey.getComponents().size mustEqual 2
      rowKey.getComponents()(0).getName mustEqual "a"
      rowKey.getComponents()(0).getType mustEqual ComponentType.STRING // important
      rowKey.getComponents()(1).getName mustEqual "b"
      rowKey.getComponents()(1).getType mustEqual ComponentType.INTEGER
      rowKey.getRangeScanStartIndex mustEqual 1
      rowKey.getNullableStartIndex mustEqual 1

      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
    }

    "create a table using an explicit THROUGH of the first element" in {
      // it shouldn't have an effect to say THROUGH A, but let's check.
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT (a, b INT, HASH(THROUGH a))
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage';
      """.stripMargin);
      res.successful mustEqual true
      res.get must beAnInstanceOf[CreateTableCommand]
      val env2 = res.get.exec()

      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val maybeLayout2 = env.kijiSystem.getTableLayout(
          defaultURI, "foo")
      maybeLayout2 must beSome[KijiTableLayout]
      val layout2 = maybeLayout2.get.getDesc
      val rowKey = maybeLayout2.get.getDesc.getKeysFormat().asInstanceOf[RowKeyFormat2]
      rowKey.getSalt().getHashSize() mustEqual 2
      rowKey.getSalt().getSuppressKeyMaterialization() mustEqual false
      rowKey.getComponents().size mustEqual 2
      rowKey.getComponents()(0).getName mustEqual "a"
      rowKey.getComponents()(0).getType mustEqual ComponentType.STRING
      rowKey.getComponents()(1).getName mustEqual "b"
      rowKey.getComponents()(1).getType mustEqual ComponentType.INTEGER
      rowKey.getRangeScanStartIndex mustEqual 1
      rowKey.getNullableStartIndex mustEqual 1

      val locGroups2 = layout2.getLocalityGroups()
      locGroups2.size mustEqual 1
    }

    "refuse to drop the current instance" in {
      val curInstance = env.instanceURI.getInstance()
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "DROP INSTANCE '" + curInstance + "';")
      res.successful mustEqual true
      res.get must beAnInstanceOf[DropInstanceCommand]
      res.get.exec() must throwA[DDLException] // Cannot drop the current instance.

      ok("Completed test")
    }
  }

  def getParser(): DDLParser = { new DDLParser(env) }

  /**
   * Given a schema id, turn it into its string representation.
   */
  def getSchemaTextFor(env: Environment, avroSchema: AvroSchema): String = {
    return env.kijiSystem.getSchemaFor(env.instanceURI, avroSchema).get.toString
  }

  /**
   * Create a table and return the Environment object where it exists.
   */
  def createBasicTable(): Environment = {
    val parser = getParser()
    val res = parser.parseAll(parser.statement, """
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
        |    age "int")
        |);
    """.stripMargin);
    return res.get.exec()
  }
}
