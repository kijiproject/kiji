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

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonParser

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro._

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment

/**
 * Print the existing layout for the table.
 */
@ApiAudience.Private
final class DescribeTableCommand(
    val env: Environment,
    val tableName: String,
    val extended: Boolean) extends TableDDLCommand {

  private val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
  private val jsonParser = new JsonParser()

  override def validateArguments(): Unit = { }
  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = { }

  override def exec(): Environment = {
    val layout = getInitialLayout()
    echo("Table: " + tableName + " (" + layout.getDescription() + ")")
    if (extended) {
      echo("Table layout format: " + layout.getVersion())
    }

    describeRowKey(layout)
    describeTableProperties(layout)
    layout.getLocalityGroups.foreach { localityGroup =>
      localityGroup.getFamilies.foreach { family =>
        Option(family.getMapSchema()) match {
          case Some(schema) => describeMapFamily(family, localityGroup)
          case None => describeGroupFamily(family, localityGroup)
        }
      }
    }

    if (extended) {
      layout.getLocalityGroups.foreach { localityGroup =>
        echo("Locality group: " + localityGroup.getName())
        val localityGroupDesc =
            localityGroup.getDescription().trim().replace("""\W+""", " ")
        echo("\tDescription: " + localityGroupDesc)
        echo("\tIn memory: " + localityGroup.getInMemory().toString())
        echo("\tMax versions: " + localityGroup.getMaxVersions().toString())
        echo("\tttl: " + localityGroup.getTtlSeconds() + " seconds")
        echo("\tCompression: " + localityGroup.getCompressionType().toString())
        echo("\tBloom filter: " + localityGroup.getBloomType())
        echo("\tBlock size: " + localityGroup.getBlockSize())
        echo("")
      }
    }

    return env
  }

  def describeRowKey(layout: TableLayoutDesc.Builder): Unit = {
    layout.getKeysFormat() match {
      case rkf: RowKeyFormat => {
        rkf.getEncoding() match {
          case RowKeyEncoding.RAW => echo("Row key: (raw bytes)")
          case RowKeyEncoding.HASH => echo("Row key: (hashed string)")
          case RowKeyEncoding.HASH_PREFIX => {
            echoNoNL("Row key: hash-prefixed string (prefix size=")
            echoNoNL(rkf.getHashSize().toString())
            echo(")")
          }
          case _ => throw new DDLException("Unexpected RowKeyEncoding in RowKeyFormat")
        }
      }
      case rkf2: RowKeyFormat2 => {
        rkf2.getEncoding() match {
          case RowKeyEncoding.RAW => echo("Row key: (raw bytes)")
          case RowKeyEncoding.FORMATTED => {
            echo("Row key:")
            rkf2.getComponents().zipWithIndex.foreach { case (component, idx) =>
              echoNoNL("\t" + component.getName() + ": " + component.getType().toString())
              if (idx < rkf2.getNullableStartIndex()) {
                echo(" NOT NULL")
              } else {
                echo("")
              }
            }
            if (extended) {
              Option(rkf2.getSalt()) match {
                case None => { echo("  (no hashing)") }
                case Some(salt: HashSpec) => {
                  echo("\tHash size: " + salt.getHashSize().toString())
                  echo("\tHashed through '" +
                      rkf2.getComponents()(rkf2.getRangeScanStartIndex() - 1).getName() + "'")
                  if (salt.getSuppressKeyMaterialization()) {
                    echo("\tKey materialization = false (hash only)")
                  }
                }
              }
            }
          }
          case _ => throw new DDLException("Unexpected RowKeyEncoding in RowKeyFormat2: "
              + layout.getKeysFormat())
        }
      }
      case _ => {
        throw new DDLException("Unexpected row key format: " + layout.getKeysFormat())
      }
    }
    echo("")
  }

  def describeTableProperties(layout: TableLayoutDesc.Builder): Unit = {
    if (!extended) {
      return
    }

    echo("Max file size: " + layout.getMaxFilesize())
    echo("Memstore flush size: " + layout.getMemstoreFlushsize())
  }

  def describeGroupFamily(groupFamily: FamilyDesc, localityGroup: LocalityGroupDesc): Unit = {
    val groupFamilyName = groupFamily.getName().trim().replace('\n', ' ')
    echo("Column family: " + groupFamilyName)
    if (extended) {
      echo("\tIn locality group: " + localityGroup.getName())
    }
    val famDescription = groupFamily.getDescription()
        .trim().replaceAll("""\s+""", " ")
    echo("\tDescription: " + famDescription)
    echo("")
    groupFamily.getColumns.foreach { column =>
      val colName = column.getName().trim().replace('\n', ' ')
      val colDescription = column.getDescription()
          .trim().replaceAll("""\s+""", " ")
      echo("\tColumn " + groupFamilyName + ":" + colName + " (" + colDescription + ")")
      if (column.getColumnSchema.getType.equals(SchemaType.COUNTER)) {
        echo("\t\tSchema: (counter)")
      } else {
        val jsonSchema = jsonParser.parse(column.getColumnSchema.getValue)
        echo("\t\tSchema: " + jsonSchema)
      }
      echo("")
    }
  }

  def describeMapFamily(mapFamily: FamilyDesc, localityGroup: LocalityGroupDesc): Unit = {
    val mapFamilyName = mapFamily.getName().trim().replace('\n', ' ')
    echo("Column family: " + mapFamilyName + " (Map-type)")
    if (extended) {
      echo("\tIn locality group: " + localityGroup.getName())
    }
    val famDescription = mapFamily.getDescription()
        .trim().replaceAll("""\s+""", " ")
    echo("\tDescription: " + famDescription)
    if (mapFamily.getMapSchema.getType.equals(SchemaType.COUNTER)) {
        echo("\tSchema: (counter)")
    } else {
      val jsonSchema = jsonParser.parse(mapFamily.getMapSchema.getValue)
      echo("\tSchema: " + jsonSchema)
    }
    echo("")
  }
}
