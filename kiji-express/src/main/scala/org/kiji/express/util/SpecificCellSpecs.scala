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

package org.kiji.express.util

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.HashMap
import java.util.{Map => JMap}
import java.util.Properties

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.avro.specific.SpecificRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.SchemaSpec
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.CellSpec
import org.kiji.schema.layout.KijiTableLayout

import SchemaSpec.Specific

@ApiAudience.Framework
@ApiStability.Experimental
object SpecificCellSpecs {
  val CELLSPEC_OVERRIDE_CONF_KEY: String = "kiji.express.input.cellspec.overrides"

  /**
   * Convert a map from field names to column input spec into a serialized map from column names to
   * specific AvroRecord class names to use as overriding reader schemas when reading from those
   * columns.
   *
   * @param columns a mapping from field name to column input spec. The column name and overriding
   *     AvroRecord class name from each column input spec will be used to populate the output
   *     serialized map.
   * @return a serialized form of a map from column name to AvroRecord class name.
   */
  def serializeOverrides(
      columns: Map[String, ColumnInputSpec]
  ): String = {
    val serializableOverrides = collectOverrides(columns)
        .map { entry: (KijiColumnName, Class[_ <: SpecificRecord]) =>
          val (key, value) = entry

          (key.toString, value.getName)
        }
        .toMap

    return serializeMap(serializableOverrides)
  }

  /**
   * Deserialize an XML representation of a mapping from column name to AvroRecord class name and
   * create a mapping from KijiColumnName to CellSpec which can be used by a KijiTableReader
   * constructor to override reader schemas for the given columns.
   *
   * @param table the KijiTable from which to retrieve base CellSpecs.
   * @param serializedMap the XML representation of the columns for which to override reader schemas
   *     and the associated AvroRecord classes to use as reader schemas.
   * @return a map from column name to Cellspec which can be used by a KijiTableReader constructor
   *     to override reader schemas for the given columns.
   */
  def deserializeOverrides(
      table: KijiTable,
      serializedMap: String
  ): Map[KijiColumnName, CellSpec] = {
    return innerBuildCellSpecs(table.getLayout, deserializeMap(serializedMap))
  }

  /**
   * Merge generic and specific CellSpecs favoring specifics.
   *
   * @param generics complete mapping from all columns to associated generic CellSpecs.
   * @param specifics mapping of columns whose reader schemas should be overridden by specific Avro
   *      classes as specified in the associated CellSpecs.
   * @return a mapping from column name to CellSpec containing all mappings from specifics and all
   *      other mappings from generics.
   */
  def mergeCellSpecs(
      generics: JMap[KijiColumnName, CellSpec],
      specifics: Map[KijiColumnName, CellSpec]
  ): JMap[KijiColumnName, CellSpec] = {
    // This JMap is necessary instead of generics.putAll(specifics) because generics is cached in
    // GenericCellSpec.
    val merged: JMap[KijiColumnName, CellSpec] = new HashMap[KijiColumnName, CellSpec]
    merged.putAll(generics)
    merged.putAll(specifics.asJava)
    return merged
  }

  /**
   * Build overridden CellSpecs for a given set of ColumnInputSpec.
   *
   * @param layout is the layout of the KijiTable from which base CellSpecs are drawn.
   * @param columns are the ColumnInputSpecs from which to build overriding CellSpecs.
   * @return a mapping from KijiColumnName to overriding CellSpec for that column.
   */
  def buildCellSpecs(
      layout: KijiTableLayout,
      columns: Map[String, ColumnInputSpec]
  ): Map[KijiColumnName, CellSpec] = {
    return innerBuildCellSpecs(layout, collectOverrides(columns))
  }

  /**
   * Collects specific AvroRecord classes to use as overriding reader schemas. Input map keys are
   * ignored.  Output map keys will be column names retrieved from the ColumnInputSpec and output
   * map values will be AvroRecord classes to use as overriding reader schemas for associated
   * columns.
   *
   * @param columns a mapping from field name to ColumnInputSpec.
   * @return a mapping from column name to the AvroRecord class to use as the reader schema when
   *     reading values from that column.
   */
  private def collectOverrides(
      columns: Map[String, ColumnInputSpec]
  ): Map[KijiColumnName, Class[_ <: SpecificRecord]] = {
    columns.values
        // Need only those columns that have specific Avro classes defined
        .map { col => (col.columnName, col.schemaSpec) }
        .collect { case (name, Specific(klass)) => (name, klass) }
        .toMap
  }

  /**
   * Serialize a map from column name to AvroRecord class name into an XML string for storage in the
   * job configuration.
   *
   * @param mapToSerialize the map from column name to AvroRecord class name.
   * @return a serialized version of these reader schema overrides.
   */
  private def serializeMap(
      mapToSerialize: Map[String, String]
  ): String = {
    val props: Properties = new Properties()
    // Add all map entries to the props.
    mapToSerialize
        .foreach {
          case (column: String, avroClass: String) => props.setProperty(column, avroClass)
        }
    // Write the properties to an XML string.
    val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    props.storeToXML(
        outputStream,
        "These properties represent specific AvroRecord reader schema overrides. "
        + "Keys are columns, values are specific AvroRecord classes.",
        "UTF-8")
    return outputStream.toString("UTF-8")
  }


  /**
   * Deserializes an XML representation of a mapping from columns to AvroRecord classes.
   *
   * @param serializedMap the XML representation of the map.
   * @return a mapping from KijiColumnName to SpecificRecord class.
   */
  private def deserializeMap(
      serializedMap: String
  ): Map[KijiColumnName, Class[_ <: SpecificRecord]] = {
    // Load the properties from the serialized xml string.
    val props: Properties = new Properties()
    props.loadFromXML(new ByteArrayInputStream(serializedMap.getBytes))

    return props.stringPropertyNames().asScala
        .map {
          case (column: String) => {
            val kcn: KijiColumnName = new KijiColumnName(column)
            val avroClass: Class[_ <: SpecificRecord] = avroClassForName(props.getProperty(column))
            (kcn, avroClass)
          }
        }
        .toMap
  }

  /**
   * Constructs CellSpecs from a KijiTableLayout and a collection of reader schema overrides.
   *
   * @param layout the table layout from which to retrieve base CellSpecs.
   * @param overrides a mapping from column to overriding reader schema.
   * @return a mapping from column name to CellSpec which can be used in a KijiTableReader
   *     constructor to override reader schemas.
   */
  private def innerBuildCellSpecs(
      layout: KijiTableLayout,
      overrides: Map[KijiColumnName, Class[_ <: SpecificRecord]]
  ): Map[KijiColumnName, CellSpec] = {
    return overrides
        .map { entry: (KijiColumnName, Class[_ <: SpecificRecord]) =>
          val (column, avroClass) = entry

          (column, layout.getCellSpec(column).setSpecificRecord(avroClass))
        }
  }

  /**
   * Gets the AvroRecord Class for a given classname.
   *
   * @param className the name of the Class to retrieve.
   * @return the AvroRecord Class for the given name.
   */
  private def avroClassForName(
      className: String
  ): Class[_ <: SpecificRecord] = {
    return Class.forName(className).asSubclass(classOf[SpecificRecord])
  }
}
