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

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.util.ArrayList
import java.text.ParseException

import scala.collection.JavaConversions._
import scala.collection.Seq
import scala.collection.mutable.Buffer
import scala.collection.mutable.Map
import scala.collection.mutable.Set

import com.google.gson.Gson

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.kiji.mapreduce.lib.bulkimport.CSVBulkImporter
import org.kiji.mapreduce.lib.bulkimport.DescribedInputTextBulkImporter
import org.kiji.mapreduce.lib.util.CSVParser
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.ColumnName

/**
 * A complete specification of a <tt>MAP FIELDS ...</tt> clause used in a DDL-based
 * bulk import for "described text."
 */
class FieldMapping(val env: Environment, val fieldNames: Option[List[String]],
    val mappings: List[FieldMappingElem]) {

  /** @return the filename of the field mapping JSON file in HDFS. */
  val jsonFilename: Path = new Path("kiji-import-" + System.currentTimeMillis() + ".json")

  /**
   * Generate a JSON field mapping file for a bulk import job and configure the
   * job to use it.
   *
   * <p>This generates a JSON file containing a bulk import field mapping specification.
   * It then uploads the file to HDFS and returns the uploaded path. The path to the
   * file will be written in the configuration in the {{DescribedInputTextBulkImporter.CONF_FILE}}
   * property.</p>
   *
   * <p>If no header row is specified in fieldNames, this will infer a header row from
   * the first file it finds when unglobbing the input paths.
   *
   * @param conf the configuration to use to connect to HDFS, and to configure with
   *     the path to the generated file.
   * @param inputPath the path to the file(s) being bulk-imported.
   * @param tableName the table being imported to.
   */
  def configureJson(conf: Configuration, inputPath: String, tableName: String): Unit = {
    val jsonMapping: JsonFieldMapping = generateJson(conf, inputPath, tableName)
    val gson = new Gson()
    val jsonStr: String = gson.toJson(jsonMapping)

    env.printer.println("Writing field mapping to file: " + jsonFilename)

    val fs: FileSystem = FileSystem.get(conf)
    val truePath: Path = jsonFilename.makeQualified(fs)
    val writer = new OutputStreamWriter(fs.create(truePath, false))
    try {
      writer.write(jsonStr)
    } finally {
      writer.close()
    }

    conf.set(DescribedInputTextBulkImporter.CONF_FILE, truePath.toString())
  }

  /**
   * Convert a sequence of strings to a comma-delimited string; also removes leading
   * and trailing whitespace from the input strings.
   *
   * @param elems the input elements.
   * @return the comma-delimited string.
   */
  def seqToStr(elems: Seq[String]): String = {
    return elems.map(str => str.trim()).mkString(",")
  }

  /**
   * Return either the field names specified in <tt>fieldNames</tt>, or infer a header
   * row from the first file found when unglobbing <tt>inputPath</tt>.
   *
   * <p>Sets the field names (from either source) in the Configuration in
   * <tt>kiji.import.text.column.header_row</tt>.</p>
   *
   * <p>Right now this method only works for CSV files. This should eventually be extended
   * to handle TSV, JSON, or other formats we can parse ahead of time. If you are not
   * using CSV files, you should explicitly state the field names you expect to use.</p>
   *
   * @param conf the configuration to use when accessing HDFS, and when configuring.
   * @param inputPath the path to the bulk import source files.
   * @return the set of field names to use.
   */
  private def resolveFieldNames(conf: Configuration, inputPath: String): Buffer[String] = {
    if (!fieldNames.isEmpty) {
      val specifiedNames = fieldNames.get
      conf.set(CSVBulkImporter.CONF_INPUT_HEADER_ROW, seqToStr(specifiedNames))
      return specifiedNames.toBuffer
    }

    val fs: FileSystem = FileSystem.get(conf)
    val files: Array[FileStatus] = fs.listStatus(new Path(inputPath))
    if (null != files) {
      files.foreach { fileStatus: FileStatus =>
        if (fileStatus.isFile()) {
          val reader = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())))
          try {
            val headerRow: String = reader.readLine().trim()
            // TODO: Respect the user's choice of delimiter; this only works with commas.
            val inferredCols: Buffer[String] = CSVParser.parseCSV(headerRow)
            conf.set(CSVBulkImporter.CONF_INPUT_HEADER_ROW, seqToStr(inferredCols))
            return inferredCols
          } catch { case pe: ParseException =>
            // Error parsing the header row. Return an empty list.
            // Since this inference method may have been called on the wrong file type,
            // don't just fail the job. The bulk importer may be able to infer its own field names.
            env.printer.println("Error parsing header row: " + pe.getMessage())
            env.printer.println("You may need to explicitly specify your input fields in your")
            env.printer.println("MAP FIELDS (field1, field2, ...) AS (...) statement.")
            return Buffer[String]()
          } finally {
            reader.close()
          }
        }
      }
    }

    throw new DDLException("Didn't find a file under the input path: " + inputPath)
  }

  /**
   * Add a binding to the field-&gt;column mapping.
   *
   * @param familyMap a map from family names to JsonFamily objects, that contains
   *    the mappings; this map will be augmented with a new family if necessary; the
   *    family specified by <tt>col.family</tt> will have a new element in it for
   *    <tt>col.qualifier</tt>.
   * @param col the output column to bind to.
   * @param field the name of the input field to map.
   */
  private def addCol(familyMap: Map[String, JsonFamily], col: ColumnName, field: String): Unit = {
    if (!familyMap.contains(col.family)) {
      // Add this family to the map from names to families, if we haven't seen it yet.
      val newJsonFam = new JsonFamily()
      newJsonFam.name = col.family
      newJsonFam.columns = new ArrayList[JsonColumn]()
      familyMap.put(col.family, newJsonFam)
    }

    // Get a handle to the family, and add this field->qualifier mapping.
    val jsonFam = familyMap.get(col.family).get
    val jsonCol = new JsonColumn()
    jsonCol.name = col.qualifier
    jsonCol.source = field
    jsonFam.columns.add(jsonCol)
  }

  /**
   * Generate an object representing the field mappings suitable for json-ification
   * and return it.
   *
   * @param conf a configuration that can be used to access HDFS.
   * @param inputPath the path to the file(s) being bulk imported.
   * @param tableName the table being imported to.
   * @return the object that can be converted to JSON by the Gson library.
   */
  def generateJson(conf: Configuration, inputPath: String,
      tableName: String): JsonFieldMapping = {

    val finalFieldNames: Buffer[String] = resolveFieldNames(conf, inputPath)
    val familyMap: Map[String, JsonFamily] = Map() // Collection of col families to include.
    val usedFields: Set[String] = Set() // Input fields we already used.
    val jsonObj = new JsonFieldMapping()

    // Column family to use by default for unmapped field names.
    var defaultMappingFam: Option[String] = None
    mappings.foreach { elem: FieldMappingElem =>
      elem match {
        case single: SingleFieldMapping => {
          addCol(familyMap, single.col, single.fieldName)
          usedFields.add(single.fieldName)
        }
        case default: DefaultFamilyMapping => {
          // Save the default family for later, after we iterate over the specific bindings.
          if (defaultMappingFam.isEmpty) {
            defaultMappingFam = Some(default.familyName)
          } else {
            throw new DDLException("Default family mapping set more than once")
          }
        }
        case eid: EntityFieldMapping => {
          jsonObj.entityIdSource = eid.fieldName
          usedFields.add(eid.fieldName)
        }
        case ts: TimestampFieldMapping => {
          jsonObj.overrideTimestampSource = ts.fieldName
          usedFields.add(ts.fieldName)
        }
        case _ => sys.error("Unbound FieldMappingElem in FieldMapping typecase.")
      }

    }

    if (!defaultMappingFam.isEmpty) {
      // Put any unmapped fields into the default mapping family.
      val defaultFamilyName = defaultMappingFam.get
      finalFieldNames.foreach { field =>
        if (!usedFields.contains(field)) {
          addCol(familyMap, new ColumnName(defaultFamilyName, field), field)
        }
      }
    }

    // Iterate over the familyMap, and convert it into a list of JsonFamily
    // objects that can be held by the top-level JsonFieldMapping.
    jsonObj.name = tableName
    jsonObj.families = new ArrayList()
    familyMap.foreach { case(_, family) =>
      jsonObj.families.add(family)
    }

    return jsonObj
  }

  /** {@inheritDoc} */
  override def equals(other: Any): Boolean = {
    // We consciously avoid 'Environment' here. It's only used for the printer in this class.

    other match {
      case fm: FieldMapping => {
        return (fieldNames.equals(fm.fieldNames)
            && mappings.equals(fm.mappings))
      }
      case _ => { return false /* wrong type: no match */ }
    }
  }
}

/**
 * Top-level object describing a field mapping JSON file.
 */
class JsonFieldMapping {
  var name: String = null
  var families: ArrayList[JsonFamily] = null
  var entityIdSource: String = null
  var overrideTimestampSource: String = null
  var version: String = "import-1.0"
}

/**
 * Object describing mappings within a column family in a JSON file.
 */
class JsonFamily {
  var name: String = null
  var columns: ArrayList[JsonColumn] = null
}

/** Object describing the mapping of a particular column in a JSON file. */
class JsonColumn {
  var name: String = null
  var source: String = null
}


