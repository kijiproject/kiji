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

import scala.collection.JavaConversions._

import org.specs2.mutable._

import org.apache.hadoop.conf.Configuration

import org.kiji.schema.KConstants
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.KijiSystem
import org.kiji.schema.shell.ddl.ColumnName
import org.kiji.schema.shell.input.NullInputSource
import org.kiji.schema.shell.spi.ParserPluginTestKit

/** Tests that parser plugins work. */
class TestFieldMapping extends SpecificationWithJUnit {
  "FieldMapping" should {
    "seqToStr zero elems" in {
      val fieldMapping = new FieldMapping(env(), None, List())
      fieldMapping.seqToStr(List()) mustEqual ""
    }

    "seqToStr one elem" in {
      val fieldMapping = new FieldMapping(env(), None, List())
      fieldMapping.seqToStr(List("a")) mustEqual "a"
    }

    "seqToStr a few elements" in {
      // This method trims leading and trailing whitespace too.
      val fieldMapping = new FieldMapping(env(), None, List())
      fieldMapping.seqToStr(List("a", " b", "    c ", "d   ", "e")) mustEqual "a,b,c,d,e"
    }

    "generate sane JSON for a default family" in {
      val fieldMapping = new FieldMapping(env(), Some(List("a","b","c")),
          List(new DefaultFamilyMapping("info")))

      val conf = new Configuration()
      val jsonObj: JsonFieldMapping = fieldMapping.generateJson(conf, "", "foo")
      jsonObj.name mustEqual "foo"
      jsonObj.version mustEqual "import-1.0"
      jsonObj.overrideTimestampSource must beNull
      jsonObj.entityIdSource must beNull
      jsonObj.families.size mustEqual 1
      val fam: JsonFamily = jsonObj.families.head
      fam.name mustEqual "info"
      fam.columns.size mustEqual 3
      val names = fam.columns.map { col: JsonColumn =>
        (col.name mustEqual col.source)
        col.name
      }

      // The names in the mapped columns should just reflect all the names in this obj.
      names mustEqual List("a", "b", "c")
    }

    "generate sane JSON with ts and entity specified" in {
      val fieldMapping = new FieldMapping(env(), Some(List("a","b","c")),
          List(new SingleFieldMapping("a", new ColumnName("info", "aaa")),
               new DefaultFamilyMapping("info"), // doesn't actually get used.
               new EntityFieldMapping("b"),
               new TimestampFieldMapping("c")))

      val conf = new Configuration()
      val jsonObj: JsonFieldMapping = fieldMapping.generateJson(conf, "", "foo")
      jsonObj.name mustEqual "foo"
      jsonObj.version mustEqual "import-1.0"
      jsonObj.overrideTimestampSource mustEqual "c"
      jsonObj.entityIdSource mustEqual "b"
      jsonObj.families.size mustEqual 1

      // Only one column actually gets mapped here
      val fam: JsonFamily = jsonObj.families.head
      fam.name mustEqual "info"
      fam.columns.size mustEqual 1
      fam.columns.head.name mustEqual "aaa"
      fam.columns.head.source mustEqual "a"
    }

    "infer a column name for single undefined field" in {
      val fieldMapping = new FieldMapping(env(), Some(List("a","b","c")),
          List(new DefaultFamilyMapping("info"), // used only for 'a'.
               new EntityFieldMapping("b"),
               new TimestampFieldMapping("c")))

      val conf = new Configuration()
      val jsonObj: JsonFieldMapping = fieldMapping.generateJson(conf, "", "foo")
      jsonObj.name mustEqual "foo"
      jsonObj.version mustEqual "import-1.0"
      jsonObj.overrideTimestampSource mustEqual "c"
      jsonObj.entityIdSource mustEqual "b"
      jsonObj.families.size mustEqual 1

      // Only one column actually gets mapped here
      val fam: JsonFamily = jsonObj.families.head
      fam.name mustEqual "info"
      fam.columns.size mustEqual 1
      fam.columns.head.name mustEqual "a"
      fam.columns.head.source mustEqual "a"
    }
  }

  def env(): Environment = {
    return new Environment(
        KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build(),
        Console.out,
        new KijiSystem,
        new NullInputSource)
  }
}
