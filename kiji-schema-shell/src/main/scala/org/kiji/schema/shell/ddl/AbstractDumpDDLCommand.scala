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

package org.kiji.schema.shell.ddl

import scala.collection.JavaConversions._

import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream

import org.apache.commons.io.IOUtils

import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.CompressionType
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.SchemaType
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.shell.Environment

/**
 * Trait that is included by DDLCommand instances that implement
 * DUMP DDL statements. The methods in this class will dump a table definition
 * to stdout or another file.
 */
trait AbstractDumpDDLCommand {

  // abstract methods implemented by DDLCommand that are required to use AbstractDumpDDLCommand.

  protected def echoNoNL(s:String): Unit
  protected def echo(s: String): Unit

  def dumpLayout(layout: TableLayoutDesc): Unit = {
    echoNoNL("CREATE TABLE ")
    echo(quote(layout.getName()))
    echoNoNL("  ")
    echo(dumpDescription(layout))
    // TODO(aaron): Support all key formats and sub-specifiers.
    layout.getKeysFormat().getEncoding() match {
      case RowKeyEncoding.HASH => { echo("  ROW KEY FORMAT HASHED") }
      case RowKeyEncoding.RAW => { echo("  ROW KEY FORMAT RAW") }
      case RowKeyEncoding.HASH_PREFIX => {
        val prefixSize = layout.getKeysFormat().getHashSize()
        echo("  ROW KEY FORMAT HASH PREFIXED (" + prefixSize + ")")
      }
    }
    var first = true
    layout.getLocalityGroups().foreach { group =>
      if (!first) {
        echo(",")
      }
      dumpLocalityGroup(group)
      first = false
    }
    echo(";")
  }

  def dumpLocalityGroup(group: LocalityGroupDesc): Unit = {
    echoNoNL("  WITH LOCALITY GROUP ")
    echo(quote(group.getName()))
    echoNoNL("    ")
    echoNoNL(dumpDescription(group))
    echo(" (")
    echo("    MAXVERSIONS = " + group.getMaxVersions().toString() + ",") // TODO: "INFINITY"
    echo("    TTL = " + group.getTtlSeconds().toString() + ",") // TODO: "FOREVER"
    echo("    INMEMORY = " + group.getInMemory().toString() + ",")
    echoNoNL("    COMPRESSED WITH " + dumpCompressionType(group.getCompressionType()))
    if (group.getFamilies().size > 0) {
      group.getFamilies().foreach { family =>
        echo(",")
        Option(family.getMapSchema()) match {
          case Some(mapSchema) => dumpMapFamily(family)
          case None => dumpGroupFamily(family)
        }
      }
    }
    echoNoNL(")")
  }

  def dumpCompressionType(comp: CompressionType): String = {
    return (comp match {
      case CompressionType.NONE => "NONE"
      case CompressionType.GZ => "GZIP"
      case CompressionType.LZO => "LZO"
      case CompressionType.SNAPPY => "SNAPPY"
    })
  }

  def dumpMapFamily(family: FamilyDesc): Unit = {
    echoNoNL("    MAP TYPE FAMILY ")
    echo(quote(family.getName()))
    echoNoNL("    WITH SCHEMA ")
    dumpSchema(family.getMapSchema())
    echoNoNL("    ")
    echo(dumpDescription(family))
  }

  def dumpGroupFamily(family: FamilyDesc): Unit = {
    echoNoNL("    FAMILY ")
    echo(quote(family.getName()))
    echoNoNL("      ")
    echoNoNL(dumpDescription(family))
    echo(" (")
    var first = true
    family.getColumns().foreach { col =>
      if (!first) {
        echo(",")
      }
      dumpColumn(col)
      first = false
    }
    echoNoNL(")")
  }

  def dumpColumn(col: ColumnDesc): Unit = {
    echoNoNL("      " + quote(col.getName()))
    echoNoNL(" WITH SCHEMA ")
    dumpSchema(col.getColumnSchema())
    echoNoNL("      ")
    echoNoNL(dumpDescription(col))
  }

  def dumpSchema(schema: CellSchema): Unit = {
    // TODO(aaron): Do we support SchemaStorage methods anywhere?
    schema.getType() match {
      case SchemaType.CLASS => { echo("CLASS " + schema.getValue().trim) }
      case SchemaType.COUNTER => { echo("COUNTER") }
      case SchemaType.INLINE => { echo(schema.getValue().trim) }
    }
  }

  /** Returns the quoted description for an object with a getDescription() method. */
  def dumpDescription(d: { def getDescription(): String }): String = {
    return "WITH DESCRIPTION " + quote(Option(d.getDescription()).getOrElse("").trim)
  }

  /**
   * Return the string specified in 's', wrapped in single-quotes,
   * with internal single-quotes and backslashes escaped.
   */
  def quote(s: String): String = {
    return "'" + s.replace("\\", "\\\\").replace("\'", "\\\'") + "'"
  }
}
