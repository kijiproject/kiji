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

import scala.collection.mutable.Map
import scala.util.parsing.combinator._

import org.kiji.mapreduce.lib.bulkimport.CSVBulkImporter

import org.kiji.schema.shell.DDLParserHelpers
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.ColumnName
import org.kiji.schema.shell.ddl.DDLCommand
import org.kiji.schema.shell.spi.ParserPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory

/**
 * Bulk importer-specific parser plugin that provides bulk importer capabilities in
 * the shell.
 */
class BulkImportParserPlugin(val env: Environment) extends ParserPlugin with DDLParserHelpers {

  /**
   * Parser for a 'field name' to apply to a column/attribute/element of the input data.
   * Matches any string in <tt>'single quotes'</tt>, or any identifier-like non-keyword
   * string not in quotes.
   *
   * @return A parser returning the field name as a string.
   */
  def field_name: Parser[String] = optionallyQuotedString

  /**
   * Matches a list of fields to expect from the input data set, or the empty string
   * (The field list may be left without explicit specification): <tt>(field1, field2, ...)</tt>
   * or the empty string. The empty string is different than the empty list <tt>()</tt>. The
   * latter specifies an explicit collection of zero fields to import.
   *
   * @return Some(List[String]) listing the field names if they are specified by the user,
   *     None otherwise.
   */
  def field_list: Parser[Option[List[String]]] = (
      "("~>repsep(optionallyQuotedString, ",")<~")" ^^ (strList => Some(strList))
    | success[Any](None) ^^ (_ => None)
  )

  /**
   * A single field mapping clause. This can be any of the following:
   *
   * <ul>
   *   <li><tt>field_name =&gt; family:column</tt> - put the specified field
   *       in the target column.</li>
   *   <li><tt>field_name =&gt; $ENTITY</tt> - use the specified field as the entity Id.</li>
   *   <li><tt>field_name =&gt; $TIMESTAMP</tt> - use the specified field as the timestamp for
   *       the insert.</li>
   *   <li><tt>DEFAULT FAMILY family</tt> - For each field <tt>foo</tt>, put it in
   *       <tt>family:foo</tt>.</li>
   * </ul>
   */
  def mapping: Parser[FieldMappingElem] = (
      i("DEFAULT")~>i("FAMILY")~>familyName ^^ (name => new DefaultFamilyMapping(name))
    | field_name<~"=>"<~"$ENTITY" ^^ (field => new EntityFieldMapping(field))
    | field_name<~"=>"<~"$TIMESTAMP" ^^ (field => new TimestampFieldMapping(field))
    | field_name~("=>"~>colName) ^^ ({case ~(field, col) => new SingleFieldMapping(field, col)})
  )

  /**
   * A set of field mapping clauses, specifying a default column family, mapping a single
   * field explicitly to a particular column, mapping a field to the timestamp, or mapping
   * a field to the entity id.
   *
   * @return a list of FieldMappingElem objects defining an individual field mapping
   *    or other property of the mappings.
   */
  def mapping_list: Parser[List[FieldMappingElem]] = (
    "("~>repsep(mapping, ",")<~")"
  )

  /**
   * A field_mapping clause specifies how fields of an input data set are mapped to columns
   * in a Kiji table.
   *
   * <tt>MAP FIELDS &lt;field_list&gt; AS ( &lt;mapping_list&gt; )</tt>
   */
  def field_mapping: Parser[FieldMapping] = (
    i("MAP")~>i("FIELDS")~>field_list~(i("AS")~>mapping_list)
    ^^ ({case ~(fields, mappings) => new FieldMapping(env, fields, mappings)})
  )

  /**
   * Declares the mechanism by which the data is loaded: direct puts, or loading through
   * HFiles written in a particular HDFS path.
   */
  def via_clause: Parser[LoadVia] = (
      i("DIRECT") ^^ (_ => new LoadViaDirect())
    | i("THROUGH")~>i("PATH")~>singleQuotedString ^^ (path => new LoadViaPath(path))
  )

  /**
   * Defines a single (key, value) pair in a {{#properties}} list.
   */
  def property: Parser[(String, String)] = (
    singleQuotedString~("="~>singleQuotedString) ^^ ({case ~(k, v) => (k, v)})
  )

  /**
   * An optional <tt>PROPERTIES ( ... )</tt> clause in a bulk importer command
   * that lets you specify run-time properties.
   *
   * @return a string-to-string (mutable) map of keys and values. If this clause is completely
   *     omitted, returns an empty map.
   */
  def properties: Parser[Map[String, String]] = (
      i("PROPERTIES")~>"("~>repsep(property, ",")<~")" ^^
      ({ case propList: List[(String, String)] =>
        // Convert the list of properties into a map.
        propList.foldLeft(Map[String, String]())({ case (map, (k, v)) => map += (k -> v) })
      })
    | success[Any](None) ^^ (_ => Map[String, String]())
  )

  /**
   * The optional <tt>FORMAT 'fmtname'</tt> clause in a generic bulk load command.
   */
  def format_clause: Parser[String] = (
      i("FORMAT")~>singleQuotedString
    | success[Any](None) ^^ (_ => "text")
  )

  /** A <tt>LOAD DATA INFILE...</tt> command explicitly intended for delimited files. */
  def delimited_bulk_load_command: Parser[DDLCommand] = (
    i("LOAD")~>i("DATA")~>i("INFILE")~>singleQuotedString~
    (i("INTO")~>i("TABLE")~>tableName)~via_clause~
    opt(i("FIELDS")~>i("TERMINATED")~>i("BY")~>singleQuotedString)~
    field_mapping ^^
    ({case ~(~(~(~(filePath, tableName), via), maybeFieldSep), fieldMapping) =>
        val props: Map[String, String] = Map()
        if (!maybeFieldSep.isEmpty) {
          props += (CSVBulkImporter.CONF_FIELD_DELIMITER -> maybeFieldSep.get)
        }
        new BulkImportCommand(env, classOf[CSVBulkImporter].getName(),
            filePath, "text", tableName, via, Some(fieldMapping), props)
    })
  )

  /**
   * A <tt>LOAD DATA INFILE...</tt> command where the user has specified the bulk
   * importer class to use.
   */
  def generic_bulk_load_command: Parser[DDLCommand] = (
    i("LOAD")~>i("DATA")~>i("INFILE")~>singleQuotedString~format_clause~
    (i("INTO")~>i("TABLE")~>tableName)~via_clause~
    ("USING"~>singleQuotedString)~opt(field_mapping)~properties ^^
    ({ case ~(~(~(~(~(~(filePath, format), tableName), via), className), fieldMapping), props) =>
      new BulkImportCommand(env, className, filePath, format, tableName, via,
          fieldMapping, props)
    })
  )

  /** A bulk load command; some sort of <tt>LOAD DATA INFILE...</tt> command. */
  def bulk_load_command: Parser[DDLCommand] = (
      delimited_bulk_load_command
    | generic_bulk_load_command
  )

  /** {@inheritDoc} */
  override def command(): Parser[DDLCommand] = bulk_load_command<~";"
}
