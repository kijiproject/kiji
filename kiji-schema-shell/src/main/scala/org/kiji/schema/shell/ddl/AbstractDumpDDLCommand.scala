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
import scala.collection.mutable.Buffer

import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream
import java.util.{List => JList}

import com.google.common.collect.Lists

import org.apache.avro.Schema

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.ComponentType
import org.kiji.schema.avro.CompressionType
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.RowKeyFormat2
import org.kiji.schema.avro.SchemaType
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment

/**
 * Trait that is included by DDLCommand instances that implement
 * DUMP DDL statements. The methods in this class will dump a table definition
 * to stdout or another file.
 */
@ApiAudience.Private
trait AbstractDumpDDLCommand extends TableProperties {

  // abstract methods implemented by DDLCommand that are required to use AbstractDumpDDLCommand.

  protected def echoNoNL(s:String): Unit
  protected def echo(s: String): Unit
  protected def env(): Environment

  def dumpLayout(layout: TableLayoutDesc): Unit = {
    val cellSchemaContext = CellSchemaContext.create(env, TableLayoutDesc.newBuilder(layout))

    echoNoNL("CREATE TABLE ")
    echo(quote(layout.getName()))
    echoNoNL("  ")
    echo(dumpDescription(layout))
    KijiTableLayout.getEncoding(layout.getKeysFormat()) match {
      // Support deprecated RowKeyFormat specifications.
      case RowKeyEncoding.HASH => { echo("  ROW KEY FORMAT HASHED") }
      case RowKeyEncoding.HASH_PREFIX => {
        val prefixSize = KijiTableLayout.getHashSize(layout.getKeysFormat())
        echo("  ROW KEY FORMAT HASH PREFIXED (" + prefixSize + ")")
      }
      // RowKeyFormat2 RAW encoding is the same syntax as before.
      case RowKeyEncoding.RAW => { echo("  ROW KEY FORMAT RAW") }
      case RowKeyEncoding.FORMATTED => {
        echoNoNL("  ROW KEY FORMAT (")
        layout.getKeysFormat() match {
          case rkf2: RowKeyFormat2 => {
            rkf2.getComponents().zipWithIndex.foreach { case (component, idx) =>
              if (idx > 0) {
                echoNoNL(", ")
              }
              echoNoNL(component.getName())
              echoNoNL(" ")
              component.getType() match {
                case ComponentType.STRING => echoNoNL("STRING")
                case ComponentType.INTEGER => echoNoNL("INT")
                case ComponentType.LONG => echoNoNL("LONG")
              }
              if (rkf2.getNullableStartIndex() > idx) {
                echoNoNL(" NOT NULL")
              }
            }

            Option(rkf2.getSalt()) match {
              case None => echoNoNL(", HASH(SIZE = 0))")
              case Some(salt: HashSpec) => {
                echoNoNL(", HASH(THROUGH ")
                val rangeStartIdx = rkf2.getRangeScanStartIndex()
                rkf2.getComponents().zipWithIndex.foreach { case (component, idx) =>
                  if (idx == rangeStartIdx - 1) {
                    echoNoNL(component.getName())
                  }
                }
                echoNoNL(", SIZE = ")
                echoNoNL(salt.getHashSize().toString())
                if (salt.getSuppressKeyMaterialization()) {
                  echoNoNL(", SUPPRESS FIELDS")
                }
                echo("))")
              }
            }
          }
          case _ => throw new DDLException("Unexpected key format")
        }
      }
    }
    dumpTableProperties(layout)
    var first = true
    layout.getLocalityGroups().foreach { group =>
      if (!first) {
        echo(",")
      }
      dumpLocalityGroup(group)
      first = false
    }
    echo(";")

    if (cellSchemaContext.supportsLayoutValidation()) {
      dumpColumnSchemas(layout)
    }
  }

  def dumpTableProperties(layout: TableLayoutDesc): Unit = {
    val numRegions: String = env.kijiSystem.getMeta(env.instanceURI, layout.getName(),
        RegionCountMetaKey).getOrElse("1")

    echo("  PROPERTIES (")
    echo("    MAX FILE SIZE = " + layout.getMaxFilesize() + ",")
    echo("    MEMSTORE FLUSH SIZE = " + layout.getMemstoreFlushsize() + ",")
    val cellSchemaContext = CellSchemaContext.create(env, TableLayoutDesc.newBuilder(layout))
    if (cellSchemaContext.supportsLayoutValidation()) {
      val validationPolicy: String = env.kijiSystem.getMeta(env.instanceURI, layout.getName(),
          TableValidationMetaKey).getOrElse(DefaultValidationPolicy)
      echo("    VALIDATION = " + validationPolicy + ",")
    }
    echo("    NUMREGIONS = " + numRegions)
    echo("  )")
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
    echo("    BLOCK SIZE = " + group.getBlockSize() + ",")
    val bloomType = group.getBloomType()
    if (null != bloomType) {
      echo("    BLOOM FILTER = " + bloomType + ",")
    } else {
      echo("    BLOOM FILTER = NONE,")
    }
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
    dumpSchema(col.getColumnSchema())
    echoNoNL("      ")
    echoNoNL(dumpDescription(col))
  }

  def dumpSchema(schema: CellSchema): Unit = {
    // TODO(aaron): Do we support SchemaStorage methods anywhere?
    schema.getType() match {
      case SchemaType.CLASS => { echo("    WITH SCHEMA CLASS " + schema.getValue().trim) }
      case SchemaType.COUNTER => { echo("    WITH SCHEMA COUNTER") }
      case SchemaType.INLINE => { echo(schema.getValue().trim) }
      case SchemaType.AVRO => {
        if (schema.getSpecificReaderSchemaClass() != null) {
          echo("    WITH SCHEMA CLASS " + schema.getSpecificReaderSchemaClass())
        } else if (schema.getDefaultReader() != null) {
          val avroSchema: Schema =
              env.kijiSystem.getSchemaFor(env.instanceURI, schema.getDefaultReader()).get
          echo("    WITH SCHEMA " + avroSchema.toString())
        } else {
          // Do nothing right now; we add non-default schemas with ALTER TABLE statements.
        }
      }
    }
  }

  /** Returns the quoted description for an object with a getDescription() method. */
  def dumpDescription(d: { def getDescription(): String }): String = {
    return "WITH DESCRIPTION " + quote(Option(d.getDescription()).getOrElse("").trim)
  }

  /**
   * Dump a series of ALTER TABLE .. ADD SCHEMA statements describing all the columns
   * in the table.
   *
   * @param the layout of the table whose columns we should dump.
   */
  def dumpColumnSchemas(layout: TableLayoutDesc): Unit = {
    layout.getLocalityGroups().foreach { group =>
      group.getFamilies().foreach { family =>
        Option(family.getMapSchema()) match {
          case Some(mapSchema) => {
            dumpAddSchema(layout.getName(), mapSchema, family.getName(), None)
          } case None => {
            // Iterate over all columns in the group-type family.
            family.getColumns().foreach { col =>
              dumpAddSchema(layout.getName(), col.getColumnSchema(),
                  family.getName(), Some(col.getName()))
            }
          }
        }
      }
    }
  }

  /**
   * Dump the ALTER TABLE .. ADD SCHEMA statements for an individual family or qualifier.
   *
   * @param the name of the table we're dumping.
   * @param the CellSchema specifying the schema set to dump
   * @param the family name to alter.
   * @param the qualifier to alter. If None, implies a map-type family.
   */
  def dumpAddSchema(tableName: String, cellSchema: CellSchema, familyName: String,
      qualifier: Option[String]): Unit = {
    if (cellSchema.getType() != SchemaType.AVRO) {
      // Nothing to do for COUNTER, INLINE, or CLASS.
      return
    }

    // Format the column name we're modifying to a string.
    val colName: String = {
      qualifier match {
        case Some(qual: String) => "COLUMN " + quote(familyName) + ":" + quote(qual)
        case None => "FAMILY " + quote(familyName)
      }
    }

    // Dump an ALTER TABLE to add each reader and writer schema in turn.
    val readers: JList[AvroSchema] = Option(cellSchema.getReaders())
        .getOrElse(Lists.newArrayList())
    readers.foreach { reader: AvroSchema =>
      val schema: Schema = env.kijiSystem.getSchemaFor(env.instanceURI, reader).get
      echo("ALTER TABLE " + quote(tableName) + " ADD READER SCHEMA " + schema + " FOR "
          + colName + ";")
    }

    val writers: JList[AvroSchema] = Option(cellSchema.getWriters())
        .getOrElse(Lists.newArrayList())
    writers.foreach { writer: AvroSchema =>
      val schema: Schema = env.kijiSystem.getSchemaFor(env.instanceURI, writer).get
      echo("ALTER TABLE " + quote(tableName) + " ADD WRITER SCHEMA " + schema + " FOR "
          + colName + ";")
    }

    // Set the specific reader schema if available.

    // Set the classname first. The resulting ALTER TABLE will set default_reader to
    // use the schema from the class present on the end user's machine.
    val specificClassName: Option[String] = Option(cellSchema.getSpecificReaderSchemaClass())
    if (specificClassName.isDefined) {
      echo("ALTER TABLE " + quote(tableName) + " ADD DEFAULT READER SCHEMA CLASS "
          + specificClassName.get + " FOR " + colName + ";")
    }

    // If the original CellSchema had a JSON default_reader field set, we should set that
    // as the final word on the default reader schema. This will override the JSON from the
    // class-based definition, but not the class name (as long as this reader schema has the
    // same class name as the class name).
    Option(cellSchema.getDefaultReader()) match {
      case Some(defaultReader) => {
        val schema: Schema = env.kijiSystem.getSchemaFor(env.instanceURI, defaultReader).get
        echo("ALTER TABLE " + quote(tableName) + " ADD DEFAULT READER SCHEMA "
            + schema.toString + " FOR " + colName + ";")
      }
      case None => ()
    }
  }

  /**
   * Return the string specified in 's', wrapped in single-quotes,
   * with internal single-quotes and backslashes escaped.
   */
  def quote(s: String): String = {
    return "'" + s.replace("\\", "\\\\").replace("\'", "\\\'") + "'"
  }
}
