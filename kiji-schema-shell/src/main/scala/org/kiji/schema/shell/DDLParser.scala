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

import scala.collection.mutable.Map
import scala.util.parsing.combinator._


import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.BloomType
import org.kiji.schema.KConstants
import org.kiji.schema.avro.AvroValidationPolicy
import org.kiji.schema.shell.ddl._
import org.kiji.schema.shell.ddl.CompressionTypeToken._
import org.kiji.schema.shell.ddl.key._
import org.kiji.schema.shell.ddl.key.RowKeyElemType._
import org.kiji.schema.shell.spi.ParserPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory
import org.kiji.avro.dsl.AvroSchemaParsers
import org.apache.avro.Schema

/**
 * An abstract representation of where a jar library (to add to the environment's
 * libjars list) might be held.
 */
private[shell] sealed trait JarLocation

/** A single local file defined by a '/path/to/foo.jar' attribute. */
private[shell] case class LocalJarFile(val path: String) extends JarLocation

/**
 * Internal representation of a preference for how validation flags are set on new columns
 * in a table.
 */
private[shell] sealed class TableValidationPolicy(val name: String,
    val avroValidationPolicy: AvroValidationPolicy)

/** Schemas for columns in a table are not validated. */
private[shell] case object NoTableValidationPolicy extends TableValidationPolicy("NONE",
    AvroValidationPolicy.NONE)

/** Schemas for columns in a table are not validated, per legacy (KijiSchema 1.0) compatibility. */
private[shell] case object LegacyTableValidationPolicy extends TableValidationPolicy("LEGACY",
    AvroValidationPolicy.SCHEMA_1_0)

/** Schemas for columns in a table are strictly validated. */
private[shell] case object StrictTableValidationPolicy extends TableValidationPolicy("STRICT",
    AvroValidationPolicy.STRICT)

/** Schemas for columns in a table are validated in "developer mode". */
private[shell] case object DeveloperTableValidationPolicy
    extends TableValidationPolicy("DEVELOPER", AvroValidationPolicy.DEVELOPER)

/**
 * A set of flags used by ALTER TABLE.. ADD SCHEMA that indicate whether a schema is
 * to be used as a (default) reader, writer, or recorded schema. More than one flag
 * may be set 'true' at a time.
 */
private[shell] case class SchemaUsageFlags(val defaultReader: Boolean,
    val reader: Boolean, val writer: Boolean, val recorded: Boolean)

/** Schema usage flags representing no specific input. */
private[shell] object EmptySchemaUsageFlags
    extends SchemaUsageFlags(false, false, false, false)

/**
 * Schema usage flags representing a deprecated "SET SCHEMA" command on a column;
 * add the column as a reader and writer schema, and make it the default.
 */
private[shell] object AddToAllSchemaUsageFlags
    extends SchemaUsageFlags(true, true, true, true)

/**
 * Parser for a kiji-schema DDL command.
 */
@ApiAudience.Private
final class DDLParser(val env: Environment)
    extends JavaTokenParsers
    with DDLParserHelpers
    with JsonStringParser
    with AvroSchemaParsers
    with TableProperties {

  /**
   * White-spaces and comments are ignored.
   * Support C-style and shell-style comments.
   *
   * This regex matches the following 4 patterns:
   *  <li> Space characters: "\s+" </li>
   *  <li> Shell-style single-line comments: "#.*" </li>
   *  <li> C-style single-line comments: "//.*" </li>
   *  <li> C-style multi-line comments: "/*(.*)*/" </li>
   *
   * Note: the regex "(?m)" enables mutli-line support for C-style delimited comments.
   */
  protected override val whiteSpace = {
    """(\s|#.*|//.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r
  }

  /** Ignore white-spaces and comments. */
  override def skipWhitespace: Boolean = {
    return true
  }

  /** Matches a legal module name. */
  def moduleName: Parser[String] = validatedNameFromOptionallyQuotedString

  /**
   * Schemas are either of the form "CLASS com.example.classname" or are Avro JSON schemas,
   * or the word 'counter'.
   */
  def schema: Parser[SchemaSpec] = (
      i("CLASS")~>repsep(ident, ".") ^^ { parts: List[String] => new ClassSchemaSpec(parts) }
    | jsonValue ^^ { json: String => new JsonSchemaSpec(json) }
    | i("COUNTER") ^^ { _ => new CounterSchemaSpec }
    | i("ID")~>longValue ^^ { uid: Long => new UidSchemaSpec(uid) }
    | i("AVRO") ~> avroType ^^ { schema: Schema => new InlineSchemaSpec(schema) }
  )

  /**
   * Returns a SchemaSpec specifying the schema definition for a column or map family.
   *
   * Matches:
   *  * WITH SCHEMA CLASS classname
   *  * WITH SCHEMA jsonSchema
   *  * WITH SCHEMA ID idNum
   *  * WITH SCHEMA COUNTER
   *  * (empty) -&gt; requires the user define a schema later with ALTER TABLE.. ADD SCHEMA..
   */
  def schemaClause: Parser[SchemaSpec] = (
      opt(i("WITH")~>i("SCHEMA"))~>schema
    | success[Any](None) ^^ (_ => new EmptySchemaSpec)
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

  /** Matches one of the bloom filter modes supported by HBase. */
  def bloomFilter: Parser[BloomType] = (
      i("NONE") ^^ (_ => BloomType.NONE)
    | i("ROW") ^^ (_ => BloomType.ROW)
    | i("ROWCOL") ^^ (_ => BloomType.ROWCOL)
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
  | i("BLOCK")~>i("SIZE")~>"="~>intValueOrNull
    ^^ (blockSize => new LocalityGroupProp(LocalityGroupPropName.BlockSize, blockSize))
  | i("BLOOM")~>i("FILTER")~>"="~>bloomFilter
    ^^ (bloomFilter => new LocalityGroupProp(LocalityGroupPropName.BloomFilter, bloomFilter))
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
   *   <li>`ROW KEY FORMAT (name STRING)` - Hash-prefixed string field called "name"</li>
   *   <li>`ROW KEY FORMAT (name)` - Hash-prefixed string field called "name". Key
   *       fields are assumed to be strings if not otherwise specified.</li>
   *   <li>`ROW KEY FORMAT (zip INT, name STRING)` - A hash-prefixed pair of fields.
   *       Only the "zip" field is hashed.
   *       The "name" field may be null (because it is not the first field).</li>
   *   <li>`ROW KEY FORMAT (zip INT, name STRING NOT NULL)` - Specifies that the
   *       "name" field may not be null. If this property is applied to the <em>i</em>'th field,
   *       it must also be applied to all fields <em>0 &lt;= field_index &lt; i</em>.</li>
   *   <li>`ROW KEY FORMAT (name STRING, HASH (SIZE = 4))` - Specifies the size of
   *       the hash prefix for the key in bytes. The default is 2. HASH SIZE must be between
   *       0 (disabled) and 16 bytes per key.</li>
   *   <li>`ROW KEY FORMAT (name STRING, HASH (THROUGH name, SIZE = 4))` - Synonym for the
   *       previous example.</li>
   *   <li>`ROW KEY FORMAT (firstname STRING, lastname STRING, HASH (THROUGH lastname))`
   *       - Specifies that the hash prefix covers both fields. The `THROUGH` clause
   *       specifies the right-most component included in the hash. All components to the left
   *       are also hashed.</li>
   *   <li>`ROW KEY FORMAT (name STRING, HASH (SUPPRESS FIELDS))` - Specifies that
   *       the actual "name" field will not be recorded: just its hash. This is identical
   *       to the original `ROW KEY FORMAT HASHED` clause. If `SUPPRESS
   *       FIELDS` is specified, the hash size is set to 16 unless explicitly set with
   *       a `SIZE = int` clause. The "THROUGH" clause is implicitly set to include
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
   *   <li>a named, typed element. (e.g., `foo STRING`)</li>
   *   <li>a named, untyped element. (e.g., `bar`) - Uses type "STRING"</li>
   *   <li>an element with an optional `NOT NULL` qualifier (e.g., `foo STRING NOT
   *       NULL`). The first element is always implicitly NOT NULL.</li>
   *   <li>a `HASH (...)` component that specifies how the hash prefix works.<li>
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
   *   <li>`THROUGH <i>fieldname</i>` - Specify that the given field is the rightmost
   *       field included in the calculation of the hash prefix. Key field names specified
   *       in this fashion represent a left prefix of the total key. That is, given fields
   *       `(a, b, c)`, specifying `HASH (THROUGH b)` includes the first two
   *       fields in the hash. By default the left-most field is incorporated in this
   *       fashion implicitly.</li>
   *   <li>`SIZE = n` - specify how many bytes of hash prefixing to use. Default is 2</li>
   *   <li>`SUPPRESS FIELDS` - specify that no fields should be literally materialized;
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
   * Return a (String, Object) representing the "TableValidationPref" property
   * and a TableValidationPolicy instance to use.
   */
  def tableValidationProperty: Parser[(String, Object)] = (
    i("VALIDATION")~>"="~>i("NONE") ^^ (_ => (TableValidationPref, NoTableValidationPolicy))
  | i("VALIDATION")~>"="~>i("LEGACY") ^^ (_ => (TableValidationPref, LegacyTableValidationPolicy))
  | i("VALIDATION")~>"="~>i("STRICT") ^^ (_ => (TableValidationPref, StrictTableValidationPolicy))
  | i("VALIDATION")~>"="~>i("DEVELOPER") ^^
    (_ => (TableValidationPref, DeveloperTableValidationPolicy))

  )

  /**
   * Return a (String, Object) pair representing a table property name and its value to set
   * on the specified table.
   */
  def tableProperty: Parser[(String, Object)] = (
    i("MAX")~>i("FILE")~>i("SIZE")~>"="~>longValueOrNull
    ^^ (maxFileSize => (MaxFileSize, maxFileSize))
  | i("MEMSTORE")~>i("FLUSH")~>i("SIZE")~>"="~>longValueOrNull
    ^^ (memStoreFlushSize => (MemStoreFlushSize, memStoreFlushSize))
  | i("NUMREGIONS")~>"="~>intValue
    ^^ (numRegions => (InitialRegionCount, numRegions.asInstanceOf[java.lang.Integer]))
  | tableValidationProperty
  )

  /**
   * Return a map from well-defined strings to key-dependent values representing the
   * different properties that can be applied to a table.
   * This clause is optional; omission returns an empty map.
   */
  def tablePropertiesClause: Parser[Map[String, Object]] = (
    i("PROPERTIES")~>i("(")~>repsep(tableProperty, ",")<~")" ^^
    ({case propList: List[(String, Object)] =>
      // Convert the list of properties into a map
      propList.foldLeft(Map[String, Object]())({ case (map, (k, v)) => map += (k -> v) })
    })
  | success[Any](None) ^^ (_ => Map[String, Object]())
  )

  /**
   * Parser that recognizes a CREATE TABLE statement.
   */
  def createTable: Parser[DDLCommand] = (
      i("CREATE")~>i("TABLE")~>tableName~descriptionClause~rowFormatClause
      ~tablePropertiesClause
      ~i("WITH")~rep1sep(lg_clause, ",")
      ^^ ({ case ~(~(~(~(~(name, desc), rowFormat), tableProps), _), localityGroups) =>
                new CreateTableCommand(env, name, desc, rowFormat, localityGroups, tableProps)
          })
  )

  /**
   * Parser that recognizes a DROP TABLE statement.
   */
  def dropTable: Parser[DDLCommand] = (
      i("DROP")~>i("TABLE")~>tableName ^^ (t => new DropTableCommand(env, t))
  )

  /** Parser that recognizes a CREATE INSTANCE statement. */
  def createInstance: Parser[DDLCommand] = (
    i("CREATE")~>i("INSTANCE")~>instanceName ^^ (name => new CreateInstanceCommand(env, name))
  )

  /** Parser that recognizes a DROP INSTANCE statement. */
  def dropInstance: Parser[DDLCommand] = (
    i("DROP")~>i("INSTANCE")~>instanceName ^^ (name => new DropInstanceCommand(env, name))
  )

  /**
   * Parser that recognizes an ALTER TABLE.. SET tableProperty clause.
   */
  def alterTableSetProperty: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~tableProperty ^^
    ({ case ~(~(name, _), (tablePropKey, tablePropVal)) =>
      val tablePropsMap = Map[String, Object]()
      tablePropsMap += (tablePropKey -> tablePropVal)
      new AlterTableSetPropertyCommand(env, name, tablePropsMap)
    })
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
   *
   * deprecated in layout-1.3; use ALTER TABLE.. ADD SCHEMA .. FOR COLUMN instead.
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
   *
   * deprecated in layout-1.3; use ALTER TABLE.. ADD SCHEMA .. FOR COLUMN instead.
   */
  def alterColumnSchema: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("SET")~i("SCHEMA")~"="~schema~i("FOR")
    ~i("COLUMN")~colName
    ^^ ({case ~(~(~(~(~(~(~(tableName, _), _), _), schema), _), _), colName) =>
        new AlterTableSetColumnSchemaCommand(env, tableName, colName, schema)
       })
  )

  /**
   * Parses optional flags that may qualify how a schema is being used in an
   * ALTER TABLE.. ADD SCHEMA .. FOR COLUMN statement.
   *
   * <p>Valid options are:</p>
   * `[DEFAULT] READER | WRITER | RECORDED | (empty)`
   *
   * <p>empty means READER and WRITER.</p>
   *
   * @return a SchemaUsageFlags indicating the usages highlighted.
   */
  def addSchemaFlags: Parser[SchemaUsageFlags] = (
    i("DEFAULT")~>i("READER") ^^
    { _ => new SchemaUsageFlags(defaultReader=true, reader=true, writer=false, recorded=false) }
  | i("READER") ^^
    { _ => new SchemaUsageFlags(defaultReader=false, reader=true, writer=false, recorded=false) }
  | i("WRITER") ^^
    { _ => new SchemaUsageFlags(defaultReader=false, reader=false, writer=true, recorded=false) }
  | i("RECORDED") ^^
    { _ => new SchemaUsageFlags(defaultReader=false, reader=false, writer=false, recorded=true) }
  | success[Any](None) ^^
    { _ => new SchemaUsageFlags(defaultReader=false, reader=true, writer=true, recorded=false) }
  )

  /**
   * Parses optional flags that may qualify how a schema is being used in an
   * DESCRIBE.. COLUMN .. SHOW .. SCHEMAS statement.
   *
   * <p>Valid options are:</p>
   * `READER | WRITER | RECORDED`
   *
   * @return a SchemaUsageFlags indicating the usages highlighted.
   */
  def showSchemaFlags: Parser[SchemaUsageFlags] = (
    i("READER") ^^ { _ => new SchemaUsageFlags(false, true, false, false) }
  | i("WRITER") ^^ { _ => new SchemaUsageFlags(false, false, true, false) }
  | i("RECORDED") ^^ { _ => new SchemaUsageFlags(false, false, false, true) }
  )

  /**
   * Parses optional flags that may qualify how a schema is being used in an
   * ALTER TABLE.. DROP SCHEMA .. FOR COLUMN statement.
   *
   * <p>Valid options are:</p>
   * `READER | WRITER | RECORDED | (empty)`
   *
   * <p>empty means READER and WRITER.</p>
   *
   * @return a SchemaUsageFlags indicating the usages highlighted.
   */
  def dropSchemaFlags: Parser[SchemaUsageFlags] = (
    i("READER") ^^ { _ => new SchemaUsageFlags(true, true, false, false) }
  | i("WRITER") ^^ { _ => new SchemaUsageFlags(false, false, true, false) }
  | i("RECORDED") ^^ { _ => new SchemaUsageFlags(false, false, false, true) }
  | success[Any](None) ^^ { _ => new SchemaUsageFlags(true, true, true, false) }
  )

  /**
   * Parser that recognizes an ALTER TABLE .. ADD SCHEMA .. FOR FAMILY statement.
   */
  def alterFamilyAddSchema: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("ADD")~addSchemaFlags~i("SCHEMA")
    ~schema~i("FOR")~i("FAMILY")~familyName
    ^^ { case tableName ~ _ ~ schemaFlags ~ _ ~ schema ~ _ ~ _ ~ familyName =>
        new AlterTableAddFamilySchemaCommand(env, tableName, schemaFlags, familyName, schema)
    }
  )

  /**
   * Parser that recognizes an ALTER TABLE .. ADD SCHEMA .. FOR COLUMN statement.
   */
  def alterColumnAddSchema: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("ADD")~addSchemaFlags~i("SCHEMA")
    ~schema~i("FOR")~i("COLUMN")~colName
    ^^ { case tableName ~ _ ~ schemaFlags ~ _ ~ schema ~ _ ~ _ ~ colName =>
        new AlterTableAddColumnSchemaCommand(env, tableName, schemaFlags, colName, schema)
    }
  )

  /**
   * Parser that recognizes an ALTER TABLE .. DROP SCHEMA .. FOR FAMILY statement.
   */
  def alterFamilyDropSchema: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("DROP")~dropSchemaFlags~i("SCHEMA")
    ~schema~i("FOR")~i("FAMILY")~familyName
    ^^ { case tableName ~ _ ~ schemaFlags ~ _ ~ schema ~ _ ~ _ ~ familyName =>
        new AlterTableDropFamilySchemaCommand(env, tableName, schemaFlags, familyName, schema)
    }
  )

  /**
   * Parser that recognizes an ALTER TABLE .. DROP SCHEMA .. FOR COLUMN statement.
   */
  def alterColumnDropSchema: Parser[DDLCommand] = (
    i("ALTER")~>i("TABLE")~>tableName~i("DROP")~dropSchemaFlags~i("SCHEMA")
    ~schema~i("FOR")~i("COLUMN")~colName
    ^^ { case tableName ~ _ ~ schemaFlags ~ _ ~ schema ~ _ ~ _ ~ colName =>
        new AlterTableDropColumnSchemaCommand(env, tableName, schemaFlags, colName, schema)
    }
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
   * Parser that recognizes a 'MODULE (modulename)' statement.
   */
  def useModule: Parser[DDLCommand] = (
    i("MODULE")~>moduleName ^^ (name => new UseModuleCommand(env, name))
  )

  /**
   * Parser that recognizes a clause that specifies a jar location. Currently supports parsing
   * 'INFILE (path-to-local-jar-file)'
   *
   * @return a parser for jar locations.
   */
  def jarLocationClause: Parser[JarLocation] = {
    i("INFILE")~>singleQuotedString ^^ { path => LocalJarFile(path) }
  }

  /**
   * Parser that recognizes a 'USE JAR (jar-location-clause)' statement.
   *
   * @return a parser for 'USE JAR ...' statements.
   */
  def useJar: Parser[DDLCommand] = {
    i("USE")~>i("JAR")~>jarLocationClause ^^ {newJar => new UseJarCommand(env, newJar) }
  }

  /**
   * List available Kiji instances. Recognizes a SHOW INSTANCES statement.
   */
  def showInstances: Parser[DDLCommand] = (
      i("SHOW")~i("INSTANCES") ^^ (_ => new ShowInstancesCommand(env))
  )

  /**
   * List available plugin modules. Recognizes SHOW MODULES.
   */
  def showModules: Parser[DDLCommand] = (
      i("SHOW")~i("MODULES") ^^ (_ => new ShowModulesCommand(env))
  )

  /**
   * Parser that recognizes a SHOW TABLES statement.
   */
  def showTables: Parser[DDLCommand] = i("SHOW")~i("TABLES") ^^ (_ => new ShowTablesCommand(env))

  /**
   * Parser that recognizes a DESCRIBE [EXTENDED] (table) statement.
   */
  def describeTable: Parser[DDLCommand] = (
      i("DESCRIBE")~>i("EXTENDED")~>tableName
      ^^ (id => new DescribeTableCommand(env, id, true))
    | i("DESCRIBE")~>tableName
      ^^ (id => new DescribeTableCommand(env, id, false))
  )

  /**
   * Matcher for the set of schemas to show in a describeColumn command.
   *
   * SHOW [n] { READER | WRITER | RECORDED } SCHEMAS
   *
   * If `n` is not specified, default to showing up to 5 schemas.
   */
  def descColShowSchemasClause: Parser[(Int, SchemaUsageFlags)] = (
    i("SHOW")~>intValue~showSchemaFlags<~i("SCHEMAS") ^^ ({
      case intVal~schemaFlags  => (intVal, schemaFlags)
    })
  | i("SHOW")~>showSchemaFlags<~i("SCHEMAS") ^^ (schemaFlags => (5, schemaFlags))
  )


  /**
   * A statement that prints a description of a specific column within a table:
   *
   * DESCRIBE t COLUMN info:foo SHOW [n] { READER | WRITER | RECORDED } SCHEMAS
   */
  def describeColumn: Parser[DDLCommand] = (
    i("DESCRIBE")~>tableName~(i("COLUMN")~>colName)~descColShowSchemasClause
    ^^ ({ case tableName~columnName~Product2(numSchemas, schemaUsageFlags) =>
            new DescribeColumnSchemasCommand(env, tableName, columnName, numSchemas,
                schemaUsageFlags)
       })
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
    | alterTableSetProperty
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
    | alterFamilyAddSchema
    | alterColumnAddSchema
    | alterFamilyDropSchema
    | alterColumnDropSchema
    | createTable
    | describeColumn
    | describeTable
    | dropTable
    | createInstance
    | dropInstance
    | dumpTableDdl
    | dumpInstanceDdl
    | loadFile
    | useJar
    | useInstance
    | useModule
    | showInstances
    | showModules
    | showTables
  )

  /**
   * Parser that recognizes a statement followed by a ';'.
   * This is the top-level parser definition for the DDLParser class.
   * If no internally-provided statement matches, it will try to use statement
   * syntax available from plugins.
   */
  def statement: Parser[DDLCommand] = (
      statementBody <~ ";"
    | pluginParsers
  )

  /**
   * A parser that uses indirection to defer parsing to plugins.
   *
   * <p>This parser should be invoked last by the 'statement'
   * parser. Its final match will always be to a failure()
   * parser that prints an error message indicating that the statement was
   * invalid.</p>
   *
   * @return a Parser that produces a DDLCommand; the Parser will check with
   *     all registered plugins to see if they can match the entire input.
   */
  def pluginParsers: Parser[DDLCommand] = PluginParser

  /**
   * Class that implements the Parser API and processes plugins.
   */
  private object PluginParser extends Parser[DDLCommand] {
    /**
     * Evaluate a module's parser against the input string.
     *
     * @param existing the existing successful result, if any.
     * @param module the module whose parser command should be tested.
     * @param in the input string to match with the parser.
     * @return None if the parser was unccessful, otherwise Some of the ParseResult
     *     from the module's parser.
     */
    private def evalModule(existing: Option[ParseResult[DDLCommand]],
        module: ParserPluginFactory, in: Input): Option[ParseResult[DDLCommand]] = {

      existing match {
        case Some(_) => { return existing }
        case None => {
          val parserPlugin: ParserPlugin = module.create(env)
          val result = parserPlugin.parseAll(parserPlugin.command, in)
          if (result.successful) {
            // This module matched! Use its result.
            val command: DDLCommand = result.get
            command.setCurrentPlugin(module) // Bind a ref to the plugin inside the DDLCommand
                                             // in case it needs to access env extension data.
            return Some(Success(command, result.next))
          } else {
            return None
          }
        }
      }
    }

    override def apply(in: Input): ParseResult[DDLCommand] = {
      val moduleResult: Option[ParseResult[DDLCommand]] =
        (env.modules.foldLeft[Option[ParseResult[DDLCommand]]]
          (None: Option[ParseResult[DDLCommand]])
          ({ (existing: Option[ParseResult[DDLCommand]], module) =>
            evalModule(existing, module, in)
          }))

      moduleResult match {
        case Some(result) => { return result }
        case None => failure("Not a valid statement. Try 'help' for example usage.").apply(in)
      }
    }
  }
}
