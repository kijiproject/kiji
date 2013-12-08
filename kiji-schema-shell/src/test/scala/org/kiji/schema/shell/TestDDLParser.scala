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

import org.specs2.mutable._

import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.ddl._
import org.kiji.schema.shell.ddl.CompressionTypeToken._
import org.kiji.schema.shell.ddl.LocalityGroupPropName._
import org.kiji.schema.shell.input.NullInputSource

/** Tests of the parsing code. What commands do we accept and what objects do they create? */
class TestDDLParser extends SpecificationWithJUnit {
  "DDLParser" should {
    "ignore comments" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "SHOW /* comment */ TABLES;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[ShowTablesCommand]
    }

    "ignore multi-line comments" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |SHOW /* multi
          |comment */ TABLES;""".stripMargin)
      res.successful mustEqual true
      res.get must beAnInstanceOf[ShowTablesCommand]
    }

    "ignore shell-style line comment" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |  # shell style line comment
          |SHOW TABLES;""".stripMargin)
      res.successful mustEqual true
      res.get must beAnInstanceOf[ShowTablesCommand]
    }

    "ignore C-style line comment" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, """
          |  // C-style line comment
          |SHOW TABLES;""".stripMargin)
      res.successful mustEqual true
      res.get must beAnInstanceOf[ShowTablesCommand]
    }

    "parse SHOW TABLES;" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "SHOW TABLES;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[ShowTablesCommand]
    }

    "require a semicolon" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "SHOW TABLES")
      res.successful mustEqual false
    }

    "be case insensitive" in {
      val parser = getParser()
      val res1 = parser.parseAll(parser.statement, "SHOW TABLES;")
      res1.successful mustEqual true
      res1.get must beAnInstanceOf[ShowTablesCommand]

      val res2 = parser.parseAll(parser.statement, "show tables;")
      res2.successful mustEqual true
      res2.get must beAnInstanceOf[ShowTablesCommand]

      val res3 = parser.parseAll(parser.statement, "ShoW TaBleS;")
      res3.successful mustEqual true
      res3.get must beAnInstanceOf[ShowTablesCommand]
    }

    "handle DESCRIBE statements" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "DESCRIBE t;")
      res.successful mustEqual true
      res.get must beAnInstanceOf[DescribeTableCommand]
      val desc: DescribeTableCommand = res.get.asInstanceOf[DescribeTableCommand]
      desc.tableName mustEqual "t";
      desc.extended mustEqual false
    }

    "handle DESCRIBE statements with quotes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "DESCRIBE 't';")
      res.successful mustEqual true
      res.get must beAnInstanceOf[DescribeTableCommand]
      val desc: DescribeTableCommand = res.get.asInstanceOf[DescribeTableCommand]
      desc.tableName mustEqual "t";
      desc.extended mustEqual false
    }

    "handle describe extended statements with quotes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement, "describe exTENDed 't';")
      res.successful mustEqual true
      res.get must beAnInstanceOf[DescribeTableCommand]
      val desc: DescribeTableCommand = res.get.asInstanceOf[DescribeTableCommand]
      desc.tableName mustEqual "t";
      desc.extended mustEqual true
    }

    "parse bool true correctly" in {
      val parser = getParser()
      val res = parser.parseAll(parser.bool, "TRUE")
      res.successful mustEqual true
      res.get mustEqual true;
    }

    "parse bool false correctly" in {
      val parser = getParser()
      val res = parser.parseAll(parser.bool, "false")
      res.successful mustEqual true
      res.get mustEqual false;
    }

    "parse maxVersions locality property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.lg_prop, "MAXVERSIONS = 12")
      res.successful mustEqual true
      val prop = res.get
      prop must beAnInstanceOf[LocalityGroupProp]
      prop.property mustEqual LocalityGroupPropName.MaxVersions;
      println("maxversions: " + prop.value.toString())
      println("maxversions: " + prop.value.getClass.toString())
      prop.value mustEqual 12
    }

    "parse inMemory true locality property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.lg_prop, "INMEMORY = TRUE")
      res.successful mustEqual true
      val prop = res.get
      prop must beAnInstanceOf[LocalityGroupProp];
      prop.property mustEqual LocalityGroupPropName.InMemory
      prop.value mustEqual true
    }

    "parse inMemory false locality property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.lg_prop, "INMEMORY = false")
      res.successful mustEqual true
      val prop = res.get
      prop must beAnInstanceOf[LocalityGroupProp]
      prop.property mustEqual LocalityGroupPropName.InMemory
      prop.value mustEqual false
    }

    "parse compression locality property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.lg_prop, "COMPRESSED WITH LZO")
      res.successful mustEqual true
      val prop = res.get
      prop must beAnInstanceOf[LocalityGroupProp]
      prop.property mustEqual LocalityGroupPropName.Compression
      prop.value must beAnInstanceOf[CompressionTypeToken]
      prop.value mustEqual CompressionTypeToken.LZO
    }

    "parse description clause" in {
      val parser = getParser()
      val res = parser.parseAll(parser.descriptionClause, "WITH DESCRIPTION 'foo'")
      res.successful mustEqual true
      val desc = res.get
      desc mustEqual Some("foo")
    }

    "parse singleQuotedString" in {
      val parser = getParser()
      val res = parser.parseAll(parser.singleQuotedString, "'foo'")
      res.successful mustEqual true
      res.get mustEqual "foo"
    }

    "parse singleQuotedString with escaped slashes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.singleQuotedString, "'fo\\\\o'")
      res.successful mustEqual true
      res.get mustEqual "fo\\o"
    }

    "parse singleQuotedString with escaped quotes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.singleQuotedString, "'fo\\'o'")
      res.successful mustEqual true
      res.get mustEqual "fo'o"
    }

    "parse singleQuotedString with raw double-quotes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.singleQuotedString, "'fo\"o'")
      res.successful mustEqual true
      res.get mustEqual "fo\"o"
    }

    "parse singleQuotedString with escaped double-quotes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.singleQuotedString, "'fo\\\"o'")
      res.successful mustEqual true
      res.get mustEqual "fo\"o"
    }

    "parse singleQuotedString with escaped slash and quote" in {
      val parser = getParser()
      val res = parser.parseAll(parser.singleQuotedString, "'fo\\\\\\'o'")
      res.successful mustEqual true
      res.get mustEqual "fo\\'o"
    }

    "parse description clause with escaped slashes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.descriptionClause, "WITH DESCRIPTION 'fo\\\\o'")
      res.successful mustEqual true
      val desc = res.get
      desc mustEqual Some("fo\\o")
    }

    "parse description clause with escaped quotes" in {
      val parser = getParser()
      val res = parser.parseAll(parser.descriptionClause, "WITH DESCRIPTION 'fo\\'o'")
      res.successful mustEqual true
      val desc = res.get
      desc mustEqual Some("fo'o")
    }

    "parse empty description clause" in {
      val parser = getParser()
      val res = parser.parseAll(parser.descriptionClause, "")
      res.successful mustEqual true
      val desc = res.get
      desc must beNone
    }

    "parse locality group clause without properties" in {
      val parser = getParser()
      val res = parser.parseAll(parser.lg_clause,
          "LOCALITY GROUP lg WITH DESCRIPTION 'foo'")
      res.successful mustEqual true
      val lg = res.get
      lg.name mustEqual "lg"
      lg.desc mustEqual Some("foo")
      lg.props.size mustEqual 0
    }

    "parse locality group clause with one property" in {
      val parser = getParser()
      val res = parser.parseAll(parser.lg_clause,
          "LOCALITY GROUP 'group' WITH DESCRIPTION 'foo' ( MAXVERSIONS = 42 )")
      res.successful mustEqual true
      val lg = res.get
      lg.name mustEqual "group"
      lg.desc mustEqual Some("foo")
      lg.props.size mustEqual 1
      lg.props.head.property mustEqual LocalityGroupPropName.MaxVersions
      lg.props.head.value mustEqual 42
    }

    "parse create table statement" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statement,
          "CREATE TABLE t WITH DESCRIPTION 'd' " +
          "WITH LOCALITY GROUP default WITH DESCRIPTION 'some data' " +
          "( MAXVERSIONS = 312, INMEMORY = FALSE );")
      res.successful mustEqual true
      val stmt = res.get
      stmt must beAnInstanceOf[CreateTableCommand]
      val ctable = stmt.asInstanceOf[CreateTableCommand]
      ctable.tableName mustEqual "t"
      ctable.desc mustEqual Some("d")
      ctable.locGroups.size mustEqual 1
      ctable.locGroups.head.name mustEqual "default"
      ctable.locGroups.head.desc mustEqual Some("some data")
      ctable.locGroups.head.props.size mustEqual 2
      ctable.locGroups.head.props.head.property mustEqual LocalityGroupPropName.MaxVersions
      ctable.locGroups.head.props.head.value mustEqual 312
      ctable.locGroups.head.props.tail.head.property mustEqual LocalityGroupPropName.InMemory
      ctable.locGroups.head.props.tail.head.value mustEqual false
    }

    "parse use jar statement" in {
      val parser = getParser()
      val res = parser.parseAll(parser.statementBody, "USE JAR INFILE '/path/to/file.jar'")
      res.successful mustEqual true
      val command = res.get
      command must beAnInstanceOf[UseJarCommand]
      val useJarCommand = command.asInstanceOf[UseJarCommand]
      useJarCommand.newJar must beAnInstanceOf[LocalJarFile]
      val LocalJarFile(path) = useJarCommand.newJar
      path mustEqual "/path/to/file.jar"
    }

    "unescaping strings should work correctly" in {
      val parser = getParser()
      parser.unescape("foo") mustEqual "foo"
      parser.unescape("") mustEqual ""
      parser.unescape("a\\\\b") mustEqual "a\\b"
      parser.unescape("a\\\"b") mustEqual "a\"b"
      parser.unescape("a\\\\\\\\b") mustEqual "a\\\\b"
      parser.unescape("a\\\\b\\\\c") mustEqual "a\\b\\c"
      parser.unescape("a\\X") mustEqual "aX"
      parser.unescape("a\\nx") mustEqual "a\nx"
      parser.unescape("a\\XY") mustEqual "aXY"
      parser.unescape("a\\\'Y") mustEqual "a\'Y"
      parser.unescape("a\\") mustEqual "a\\"
      parser.unescape("a\\\\") mustEqual "a\\"
    }
  }

  def getParser(): DDLParser = {
    val defaultURI = KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()
    new DDLParser(new Environment(
        defaultURI,
        System.out,
        new MockKijiSystem(),
        new NullInputSource,
        List(),
        false))
  }
}
