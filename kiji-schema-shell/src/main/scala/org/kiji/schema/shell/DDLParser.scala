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

package org.kiji.schema.shell

import org.kiji.schema.avro.HashType
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.RowKeyFormat
import org.kiji.schema.KConstants

import org.kiji.schema.shell.ddl._
import org.kiji.schema.shell.ddl.CompressionTypeToken._
import org.kiji.schema.shell.ddl.LocalityGroupPropName._
import org.kiji.schema.shell.ddl.key._
import org.kiji.schema.shell.ddl.key.RowKeyElemType._

import org.kiji.schema.util.KijiNameValidator

import scala.util.parsing.combinator._

/**
 * Singleton parser object for a kiji-schema DDL command.
 */
class DDLParser(val env: Environment) extends JavaTokenParsers with JsonStringParser {

  /**
   * Matches a string enclosed by 'single quotes', that may contain escapes.
   *
   * @return a parser tha matches the above and returns the string contained within
   * the quotes, with the enclosing single-quote-marks removed and any escape character
   * sequences converted to the actual characters they represent.
   */
  def singleQuotedString: Parser[String] = (
    // Regex adapted from http://blog.stevenlevithan.com/archives/match-quoted-string
    """'(?:\\?+.)*?'""".r
    ^^ (strWithEscapes => unescape(strWithEscapes.substring(1, strWithEscapes.length - 1)))
  )

  /** An identifier that is optionally 'single quoted' */
  def optionallyQuotedString: Parser[String] = (
      ident | singleQuotedString
  )

  /**
   * Matches table, family, etc. names are strings that are optionally 'single quoted',
   * and must match the Kiji name restrictions.
   **/
  def validatedNameFromOptionallyQuotedString: Parser[String] = (
    optionallyQuotedString ^^ (name => { KijiNameValidator.validateLayoutName(name); name })
  )

  /**
   * Matches a legal Kiji instance name.
   */
  def instanceName: Parser[String] = (
    optionallyQuotedString ^^ (name => { KijiNameValidator.validateKijiName(name); name })
  )

  /** Matches a legal Kiji table name. */
  def tableName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji row key component name. */
  def rowKeyElemName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji locality group name. */
  def localityGroupName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji column family name. */
  def familyName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji column qualifier name. */
  def qualifier: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Column names take the form info:foo, 'info':foo, info:'foo', or 'info':'foo' */
  def colName: Parser[ColumnName] = (
      validatedNameFromOptionallyQuotedString~":"~validatedNameFromOptionallyQuotedString
      ^^ ({case ~(~(family, _), qualifier) => new ColumnName(family, qualifier) })
  )

  /**
   * Schemas are either of the form "CLASS com.example.classname" or are Avro JSON schemas,
   * or the word 'counter'.
   */
  def schema: Parser[SchemaSpec] = (
      i("CLASS")~>repsep(ident, ".")
      ^^ (parts => new ClassSchemaSpec(parts))
    | jsonValue ^^ (json => new JsonSchemaSpec(json))
    | i("COUNTER") ^^ (_ => new CounterSchemaSpec)
  )

  def schemaClause: Parser[SchemaSpec] = opt(i("WITH")~>i("SCHEMA"))~>schema

  def bool: Parser[Boolean] = (
      i("TRUE") ^^ (_ => true)
    | i("FALSE") ^^ (_ => false)
  )

  /**
   * Matches an integer. The strings INFINITY and FOREVER are both synonyms for Int.MaxValue.
   */
  def intValue: Parser[Int] = (
      wholeNumber ^^ (x => x.toInt)
    | i("INFINITY") ^^ (_ => Int.MaxValue)
    | i("FOREVER") ^^ (_ => Int.MaxValue)
  )

  /**
   * Matches one of the compression types supported by HBase.
   */
  def compression : Parser[CompressionTypeToken] = (
      i("NONE") ^^ (_ => CompressionTypeToken.NONE)
    | i("GZIP") ^^ (_ => CompressionTypeToken.GZIP)
    | i("LZO") ^^ (_ => CompressionTypeToken.LZO)
    | i("SNAPPY") ^^ (_ => CompressionTypeToken.SNAPPY)
  )


  /**
   * An optional clause of the form: WITH DESCRIPTION 'd'.
   *
   * @return a parser that returns a string option.
   */
  def descriptionClause: Parser[Option[String]] = (
      i("WITH")~>i("DESCRIPTION")~>singleQuotedString ^^ (str => Some(str))
    | success[Any](None) ^^ (_ => None)
  )

  /**
   * A basic_lg_prop represents a single locality group property within an lg_clause.
   */
  def basic_lg_prop: Parser[LocalityGroupProp] = (
    i("MAXVERSIONS")~>"="~>intValue
    ^^ (maxVers => new LocalityGroupProp(LocalityGroupPropName.MaxVersions, maxVers))
  | i("INMEMORY")~>"="~>bool
    ^^ (b => new LocalityGroupProp(LocalityGroupPropName.InMemory, b))
  | i("TTL")~>"="~>intValue
    ^^ (ttl => new LocalityGroupProp(LocalityGroupPropName.TimeToLive, ttl))
  | i("COMPRESSED")~>i("WITH")~>compression
    ^^ (comp => new LocalityGroupProp(LocalityGroupPropName.Compression, comp))
  )

  /** Description of a map-type column family. */
  def map_family_clause: Parser[LocalityGroupProp] = (
    i("MAP")~>i("TYPE")~>i("FAMILY")~>familyName~schemaClause~descriptionClause
    ^^ ({case ~(~(familyName, schema), desc) =>
        new LocalityGroupProp(LocalityGroupPropName.MapFamily,
            new MapFamilyInfo(familyName, schema, desc))
       })
  )

  /**
   * A column definition clause nested inside a GROUP TYPE FAMILY definition.
   */
  def col_clause: Parser[ColumnClause] = (
    opt(i("COLUMN"))~>qualifier~schemaClause~descriptionClause
    ^^ ({case ~(~(qualifier, schema), desc) => new ColumnClause(None, qualifier, schema, desc) })
  )

  /**
   * A column definition clause in an ALTER TABLE ADD COLUMN statement. Requires
   * the "COLUMN" keyword as well as the fully-qualified column name.
   */
  def long_form_col_clause: Parser[ColumnClause] = (
    i("COLUMN")~>colName~schemaClause~descriptionClause
    ^^ ({case ~(~(colName, schema), desc) =>
        new ColumnClause(Some(colName.family), colName.qualifier, schema, desc) })
  )

  /**
   * Matches a comma-delimited list of col_clause elements (surrounded in parentheses).
   */
  def columnList: Parser[List[ColumnClause]] = (
      "("~>repsep(col_clause, ",")<~")"
    | success[Any](None) ^^ (_ => List[ColumnClause]())
  )

  /** Description of a group-type column family. */
  def group_family_clause: Parser[LocalityGroupProp] = (
    opt(i("GROUP")~>i("TYPE"))~>i("FAMILY")~>familyName~descriptionClause~columnList
    ^^ ({case ~(~(familyName, desc), colList) =>
        new LocalityGroupProp(LocalityGroupPropName.GroupFamily,
            new GroupFamilyInfo(familyName, desc, colList))
       })
  )

  /**
   * Parser that matches any clauses valid as locality group properties.
   * These include the basic properties (e.g., MAXVERSIONS = INFINITY) specified
   * by basic_lg_prop, or clauses that define map- and group-type families,
   * matched by map_family_clause or group_family_clause respectively.
   */
  def lg_prop: Parser[LocalityGroupProp] = (
    basic_lg_prop | map_family_clause | group_family_clause
  )

  /**
   * Locality groups are specified in CREATE TABLE and in ALTER TABLE (locality group) stmts.
   * A locality group and all its properties are specified in an lg_clause.
   */
  def lg_clause: Parser[LocalityGroupClause] = (
      i("LOCALITY")~>i("GROUP")~>localityGroupName~descriptionClause
      ~opt("("~>repsep(lg_prop, ",")<~")")
      ^^ ({case ~(~(name, desc), props) =>
              props match {
              case Some(proplist) => new LocalityGroupClause(name, desc, proplist)
              case None => new LocalityGroupClause(name, desc, List[LocalityGroupProp]())
            }
          })
  )

  /**
   * Optional clause that specifies how the row keys are formatted in a CREATE TABLE
   * statement.
   *
   * Defaults to using a single hash-prefixed string component.
   * Also supports explicitly hashed, hash-prefixed, raw,
   * or composite/formatted keys.
   */
  def rowFormatClause: Parser[FormattedKeySpec] = (
      i("ROW")~>i("KEY")~>i("FORMAT")~>i("HASHED") ^^ (_ => new HashedFormattedKeySpec)
    | i("ROW")~>i("KEY")~>i("FORMAT")~>i("RAW")    ^^ (_ => RawFormattedKeySpec)
    | i("ROW")~>i("KEY")~>i("FORMAT")~>i("HASH")~>i("PREFIXED")~>"("~>intValue<~")"
      ^^ (size => new HashPrefixKeySpec(size))
    | i("ROW")~>i("KEY")~>i("FORMAT")~>formattedKeysClause
    | success[Boolean](true) ^^ (_ => DefaultKeySpec)
  )

  /**
   * Row key format consisting of multiple named and typed components. Allows specification
   * of additional properties regarding the row key format as well.
   *
   * <p>By default, the first element in the key will be hash-prefixed with a 2-byte MD5
   * hash. All non-initial elements may be null unless explicitly specified otherwise.
   * If a field is non-null, all fields to its left must also be non-null. You may change
   * these defaults through the specification language described below.</p>
   *
   * <p>Examples:</p>
   * <ul>
   *   <li><tt>ROW KEY FORMAT (name STRING)</tt> - Hash-prefixed string field called "name"</li>
   *   <li><tt>ROW KEY FORMAT (name)</tt> - Hash-prefixed string field called "name". Key
   *       fields are assumed to be strings if not otherwise specified.</li>
   *   <li><tt>ROW KEY FORMAT (zip INT, name STRING)</tt> - A hash-prefixed pair of fields.
   *       Only the "zip" field is hashed.
   *       The "name" field may be null (because it is not the first field).</li>
   *   <li><tt>ROW KEY FORMAT (zip INT, name STRING NOT NULL)</tt> - Specifies that the
   *       "name" field may not be null. If this property is applied to the <em>i</em>'th field,
   *       it must also be applied to all fields <em>0 &lt;= field_index &lt; i</em>.</li>
   *   <li><tt>ROW KEY FORMAT (name STRING, HASH (SIZE = 4))</tt> - Specifies the size of
   *       the hash prefix for the key in bytes. The default is 2. HASH SIZE must be between
   *       0 (disabled) and 16 bytes per key.</li>
   *   <li><tt>ROW KEY FORMAT (name STRING, HASH (THROUGH name, SIZE = 4))</tt> - Synonym for the
   *       previous example.</li>
   *   <li><tt>ROW KEY FORMAT (firstname STRING, lastname STRING, HASH (THROUGH lastname))</tt>
   *       - Specifies that the hash prefix covers both fields. The <tt>THROUGH</tt> clause
   *       specifies the right-most component included in the hash. All components to the left
   *       are also hashed.</li>
   *   <li><tt>ROW KEY FORMAT (name STRING, HASH (SUPPRESS FIELDS))</tt> - Specifies that
   *       the actual "name" field will not be recorded: just its hash. This is identical
   *       to the original <tt>ROW KEY FORMAT HASHED</tt> clause. If <tt>SUPPRESS
   *       FIELDS</tt> is specified, the hash size is set to 16 unless explicitly set with
   *       a <tt>SIZE = int</tt> clause. The "THROUGH" clause is implicitly set to include
   *       all fields if it's not already set. If you explicitly set THROUGH, it must be
   *       through the last (right-most) field.</li>
   * </ul>
   */
  def formattedKeysClause: Parser[FormattedKeySpec] = (
    "("~>rep1sep(formattedKeyParam, ",")<~")"
    ^^ (subclauses => new FormattedKeySpec(subclauses))
  )

  /**
   * Within a formatted row key, several properties can be specified:
   * <ul>
   *   <li>a named, typed element. (e.g., <tt>foo STRING</tt>)</li>
   *   <li>a named, untyped element. (e.g., <tt>bar</tt>) - Uses type "STRING"</li>
   *   <li>an element with an optional <tt>NOT NULL</tt> qualifier (e.g., <tt>foo STRING NOT
   *       NULL</tt>). The first element is always implicitly NOT NULL.</li>
   *   <li>a <tt>HASH (...)</tt> component that specifies how the hash prefix works.<li>
   * </ul>
   */
  def formattedKeyParam: Parser[FormattedKeyParam] = (
    i("HASH")~>"("~>rep1sep(keyHashClause, ",")<~")"
    ^^ (keyHashClauses => new KeyHashParams(keyHashClauses))
  | rowKeyElemName~rowKeyElemType~rowKeyElemNull
    ^^ ({case ~(~(name, elemType), elemNull) => new KeyComponent(name, elemType, elemNull)})
  )

  /**
   * Within a formatted row key, a HASH ( ... ) block contains one or more key hash
   * parameters:
   *
   * <ul>
   *   <li><tt>THROUGH <i>fieldname</i></tt> - Specify that the given field is the rightmost
   *       field included in the calculation of the hash prefix. Key field names specified
   *       in this fashion represent a left prefix of the total key. That is, given fields
   *       <tt>(a, b, c)</tt>, specifying <tt>HASH (THROUGH b)</tt> includes the first two
   *       fields in the hash. By default the left-most field is incorporated in this
   *       fashion implicitly.</li>
   *   <li><tt>SIZE = n</tt> - specify how many bytes of hash prefixing to use. Default is 2</li>
   *   <li><tt>SUPPRESS FIELDS</tt> - specify that no fields should be literally materialized;
   *       only retain the hash itself. If no SIZE argument is specified, then the hash
   *       defaults to the full 16 bytes.</li>
   * </ul>
   */
  def keyHashClause: Parser[FormattedKeyHashParam] = (
    i("THROUGH")~>rowKeyElemName ^^ (elem => new FormattedKeyHashComponent(elem))
  | i("SIZE")~>"="~>intValue ^^ (size => new FormattedKeyHashSize(size))
  | i("SUPPRESS")~>i("FIELDS") ^^ (_ => new FormattedKeySuppressFields)
  )

  /**
   * Named elements of a row key may be strings, integers, or long values. The default is
   * STRING, if left unspecified by the user.
   */
  def rowKeyElemType: Parser[RowKeyElemType] = (
    i("STRING") ^^ (_ => RowKeyElemType.STRING)
  | i("INT") ^^ (_ => RowKeyElemType.INT)
  | i("LONG") ^^ (_ => RowKeyElemType.LONG)
  | success[Any](None) ^^ (_ => RowKeyElemType.STRING) // Default key elem type is string.
  )

  /**
   * Return true if an element of a row key may be null, false otherwise.
   */
  def rowKeyElemNull: Parser[Boolean] = (
    i("NOT")~i("NULL") ^^ (_ => false)
  | success[Any](None) ^^ (_ => true) // Default is nullable.
  )

  /**
   * Parser that recognizes a CREATE TABLE statement.
   */
  def createTable: Parser[DDLCommand] = (
      i("CREATE")~>i("TABLE")~>tableName~descriptionClause~rowFormatClause
      ~i("WITH")~rep1sep(lg_clause, ",")
      ^^ ({ case ~(~(~(~(name, desc), rowFormat), _), localityGroups) =>
                new CreateTableCommand(env, name, desc, rowFormat, localityGroups)
          })
  )

  /**
   * Parser that recognizes a DROP TABLE statement.
   */
  def dropTable: Parser[DDLCommand] = (
      i("DROP")~>i("TABLE")~>tableName ^^ (t => new DropTableCommand(env, t))
  )

  /**
   * Parser that recognizes an ALTER TABLE.. ADD [GROUP TYPE] FAMILY clause.
   */
  def alterAddGroupFamily: Parser[DDLCommand] = (
      i("ALTER")~>i("TABLE")~>tableName~i("ADD")~group_family_clause
      ~i("TO")~opt(i("LOCALITY")~i("GROUP"))~localityGroupName
      ^^ ({case ~(~(~(~(~(tableName, _), groupClause), _), _), lgName) =>
          require (groupClause.property == LocalityGroupPropName.GroupFamily)
          new AlterTableAddGroupFamilyCommand(env, tableName, groupClause, lgName)
      })
  )

  /**
   * Parser that recognizes an ALTER TABLE.. ADD MAP TYPE FAMILY clause.
   */
  def alterAddMapFamily: Parser[DDLCommand] = (
      i("ALTER")~>i("TABLE")~>tableName~i("ADD")~map_family_clause
      ~i("TO")~opt(i("LOCALITY")~i("GROUP"))~localityGroupName
      ^^ ({case ~(~(~(~(~(tableName, _), mapClause), _), _), lgName) =>
            require (mapClause.property == LocalityGroupPropName.MapFamily)
            new AlterTableAddMapFamilyCommand(env, tableName, mapClause, lgName)
          })
  )

  /**
   * Parser that recognizes an ALTER TABLE ... ADD COLUMN statement.
   */
  def alterAddColumn: Parser[DDLCommand] = (
      i("ALTER")~>i("TABLE")~>tableName~i("ADD")~long_form_col_clause
      ^^ ({case ~(~(tableName, _), colClause) =>
          new AlterTableAddColumnCommand(env, tableName, colClause)
         })

  )

  /**
   * Parser that recognizes an ALTER TABLE .. DROP FAMILY statement.
   */
  def alterDropFamily: Parser[DDLCommand] = (
      i("ALTER")~>i("TABLE")~>tableName~i("DROP")~i("FAMILY")~familyName
      ^^ ({case ~(~(~(tableName, _), _), familyName) =>
          new AlterTableDropFamilyCommand(env, tableName, familyName)
         })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. DROP COLUMN statement.
   */
  def alterDropColumn: Parser[DDLCommand] = (
      i("ALTER")~>i("TABLE")~>tableName~i("DROP")~i("COLUMN")~colName
      ^^ ({case ~(~(~(tableName, _), _), colName) =>
          new AlterTableDropColumnCommand(env, tableName, colName)
         })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. DROP LOCALITY GROUP statement.
   */
  def alterDropLocalityGroup: Parser[DDLCommand] = (
      i("ALTER")~>i("TABLE")~>tableName~i("DROP")~i("LOCALITY")~i("GROUP")~localityGroupName
      ^^ ({case ~(~(~(~(tableName, _), _), _), lgName) =>
          new AlterTableDropLocalityGroupCommand(env, tableName, lgName)
         })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. RENAME FAMILY statement.
   */
  def alterRenameFamily: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("RENAME")~i("FAMILY")~familyName~opt(i("AS"))~familyName
    ^^ ({case ~(~(~(~(~(tableName, _), _), familyName), _), targetName) =>
        new AlterTableRenameFamilyCommand(env, tableName, familyName, targetName)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. RENAME COLUMN statement.
   */
  def alterRenameColumn: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("RENAME")~i("COLUMN")~colName~opt(i("AS"))~colName
    ^^ ({ case ~(~(~(~(~(tableName, _), _), colName), _), targetName) =>
        new AlterTableRenameColumnCommand(env, tableName, colName, targetName)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. RENAME LOCALITY GROUP statement.
   */
  def alterRenameLocalityGroup: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("RENAME")~i("LOCALITY")~i("GROUP")
    ~localityGroupName~opt(i("AS"))~localityGroupName
    ^^ ({ case ~(~(~(~(~(~(tableName, _), _), _), locGroupName), _), targetName) =>
        new AlterTableRenameLocalityGroupCommand(env, tableName, locGroupName, targetName)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. CREATE LOCALITY GROUP statement.
   */
  def alterCreateLocalityGroup: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("CREATE")~lg_clause
    ^^ ({case ~(~(tableName, _), locGroup) =>
        new AlterTableCreateLocalityGroupCommand(env, tableName, locGroup)
    })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. SET DESCRIPTION statement.
   */
  def alterTableDesc: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~i("DESCRIPTION")~"="~singleQuotedString
    ^^ ({case ~(~(~(~(tableName, _), _), _), desc) =>
        new AlterTableDescCommand(env, tableName, desc)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. SET DESCRIPTION .. FOR LOCALITY GROUP
   * statement.
   */
  def alterLocalityGroupDesc: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~i("DESCRIPTION")~"="~singleQuotedString
    ~i("FOR")~i("LOCALITY")~i("GROUP")~localityGroupName
    ^^ ({case ~(~(~(~(~(~(~(~(tableName, _), _), _), desc), _), _), _), localityGroupName) =>
        new AlterTableDescForLocalityGroupCommand(env, tableName, localityGroupName, desc)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. SET DESCRIPTION .. FOR FAMILY
   * statement.
   */
  def alterFamilyDesc: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~i("DESCRIPTION")~"="~singleQuotedString
    ~i("FOR")~i("FAMILY")~familyName
    ^^ ({case ~(~(~(~(~(~(~(tableName, _), _), _), desc), _), _), familyName) =>
        new AlterTableDescForFamilyCommand(env, tableName, familyName, desc)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. SET DESCRIPTION .. FOR COLUMN
   * statement.
   */
  def alterColumnDesc: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~i("DESCRIPTION")~"="~singleQuotedString
    ~i("FOR")~i("COLUMN")~colName
    ^^ ({case ~(~(~(~(~(~(~(tableName, _), _), _), desc), _), _), columnName) =>
        new AlterTableDescForColumnCommand(env, tableName, columnName, desc)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. SET (property) .. FOR LOCALITY GROUP
   * statement.
   */
  def alterLocalityGroupProperty: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~basic_lg_prop~i("FOR")~i("LOCALITY")~i("GROUP")
    ~localityGroupName
    ^^ ({case ~(~(~(~(~(~(tableName, _), lgProp), _), _), _), lgName) =>
        new AlterLocalityGroupPropertyCommand(env, tableName, lgName, lgProp)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. SET SCHEMA = .. FOR [MAP TYPE] FAMILY
   * statement.
   */
  def alterFamilySchema: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~i("SCHEMA")~"="~schema~i("FOR")
    ~opt(i("MAP")~i("TYPE"))~i("FAMILY")~familyName
    ^^ ({case ~(~(~(~(~(~(~(~(tableName, _), _), _), schema), _), _), _), familyName) =>
        new AlterTableSetFamilySchemaCommand(env, tableName, familyName, schema)
       })
  )

  /**
   * Parser that recognizes an ALTER TABLE .. SET SCHEMA = .. FOR COLUMN statement.
   */
  def alterColumnSchema: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~i("SCHEMA")~"="~schema~i("FOR")
    ~i("COLUMN")~colName
    ^^ ({case ~(~(~(~(~(~(~(tableName, _), _), _), schema), _), _), colName) =>
        new AlterTableSetColumnSchemaCommand(env, tableName, colName, schema)
       })
  )

  /**
   * Parser that recognizes a USE DEFAULT INSTANCE statement or a USE (instancename) statement.
   */
  def useInstance: Parser[DDLCommand] = (
      i("USE")~>i("DEFAULT")~>i("INSTANCE")
      ^^ (_ => new UseInstanceCommand(env, KConstants.DEFAULT_INSTANCE_NAME))
    | i("USE")~>instanceName ^^ (name => new UseInstanceCommand(env, name))
  )

  /**
   * List available Kiji instances. Recognizes a SHOW INSTANCES statement.
   */
  def showInstances: Parser[DDLCommand] = (
      i("SHOW")~i("INSTANCES") ^^ (_ => new ShowInstancesCommand(env))
  )

  /**
   * Parser that recognizes a SHOW TABLES statement.
   */
  def showTables: Parser[DDLCommand] = i("SHOW")~i("TABLES") ^^ (_ => new ShowTablesCommand(env))

  /**
   * Parser that recognizes a DESCRIBE [EXTENDED] (table) statement.
   */
  def descTable: Parser[DDLCommand] = (
      i("DESCRIBE")~>i("EXTENDED")~>tableName
      ^^ (id => new DescribeTableCommand(env, id, true))
    | i("DESCRIBE")~>tableName
      ^^ (id => new DescribeTableCommand(env, id, false))
  )

  /**
   * Parser that recognizes a DUMP DDL [TO FILE (filename)] statement.
   */
  def dumpInstanceDdl: Parser[DDLCommand] = (
    i("DUMP")~>i("DDL")~>opt(i("TO")~>i("FILE")~>singleQuotedString)
    ^^ (maybeFile => new DumpInstanceDDLCommand(env, maybeFile))
  )

  /**
   * Parser that recognizes a DUMP DDL FOR TABLE .. [TO FILE (filename)] statement.
   */
  def dumpTableDdl: Parser[DDLCommand] = (
    i("DUMP")~>i("DDL")~>i("FOR")~>i("TABLE")~>tableName
    ~opt(i("TO")~>i("FILE")~>singleQuotedString)
    ^^ ({ case ~(tableName, maybeFile) => new DumpTableDDLCommand(env, tableName, maybeFile) })
  )

  /**
   * Parser that recognizes a LOAD FROM FILE (filename) statement.
   *
   * @return a parser that returns a LoadFileCommand instance.
   */
  def loadFile: Parser[DDLCommand] = (
    i("LOAD")~>i("FROM")~>i("FILE")~>singleQuotedString
    ^^ (filename => new LoadFileCommand(env, filename))
  )

  /**
   * Parser that recognizes all legal statements in the language.
   *
   * @return a Parser that returns some DDLCommand subclass.
   */
  def statementBody: Parser[DDLCommand] = (
      alterAddGroupFamily
    | alterAddMapFamily
    | alterDropFamily
    | alterRenameFamily
    | alterAddColumn
    | alterRenameColumn
    | alterDropColumn
    | alterCreateLocalityGroup
    | alterDropLocalityGroup
    | alterRenameLocalityGroup
    | alterColumnDesc
    | alterFamilyDesc
    | alterLocalityGroupDesc
    | alterTableDesc
    | alterLocalityGroupProperty
    | alterFamilySchema
    | alterColumnSchema
    | createTable
    | descTable
    | dropTable
    | dumpTableDdl
    | dumpInstanceDdl
    | loadFile
    | useInstance
    | showInstances
    | showTables
    | failure("Not a valid statement. Try 'help' for example usage.")
  )

  /**
   * Parser that recognizes a statement followed by a ';'.
   * This is the top-level parser definition for the DDLParser class.
   */
  def statement: Parser[DDLCommand] = statementBody ~ ";" ^^ ({case ~(s, _) => s})

  /**
   * @param s the string to recognize in a case-insensitive fashion.
   * @return a parser which matches the word in 's' in a case-insensitive fashion.
   */
  protected def i(s: String): Parser[String] = {
    val sb = new StringBuilder
    s.foreach { ch =>
      sb.append('[')
      sb.append(ch.toUpper)
      sb.append(ch.toLower)
      sb.append(']')
    }
    regex(sb.toString().r)
  }

  /**
   * Given a string that contains \\ and \', convert these sequences to \ and ' respectively.
   *
   * @param s a string that may contain escape sequences to protect characters in
   * a 'single quoted string' matched by a parser.
   * @return the same string in 's' with the escapes converted to their true character
   * representations.
   */
  def unescape(s: String): String = {
    val sb = new StringBuilder
    var i: Int = 0;
    while (i < s.length) {
      if (i < s.length - 1 && s.charAt(i) == '\\') {
        s.charAt(i + 1) match {
          case '\\' => { sb.append("\\"); i += 1 }
          case '\'' => { sb.append("\'"); i += 1 }
          case 'n' => { sb.append("\n"); i += 1 }
          case 't' => { sb.append("\t"); i += 1 }
          case c => { sb.append(c); i += 1 } // Everything else escapes to itself.
        }
      } else {
        sb.append(s.charAt(i))
      }

      i += 1
    }

    return sb.toString()
  }
}
