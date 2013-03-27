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

import org.kiji.schema.shell.ddl.ColumnName

/**
 * A single clause in the main <tt>MAP FIELDS ... AS ( &lt;clauses&lt; )</tt> list.
 *
 * <p>This can specify a single field to column mapping, a default column family to load
 * to, or other properties of the import.</p>
 *
 * <p>Instances of <tt>FieldMappingElem</tt> are examined in {{FieldMapping#generateJson}};
 * if you add a new subclass, you must modify the typecase in that method.</p>
 */
abstract class FieldMappingElem

/**
 * A <tt>fieldname =&gt; family:qualifier</tt> clause in a field mapping that
 * maps the specified field into the specified target column.
 *
 * @param fieldName the name of the input field.
 * @param colName the name of the target column
 */
class SingleFieldMapping(val fieldName: String, val col: ColumnName) extends FieldMappingElem {
  /** {@inheritDoc} */
  override def equals(other: Any): Boolean = {
    other match {
      case single: SingleFieldMapping => {
        return fieldName == single.fieldName && col.equals(single.col)
      }
      case _ => { return false }
    }
  }

  /** {@inheritDoc} */
  override def hashCode(): Int = {
    return fieldName.hashCode() ^ col.hashCode()
  }
}

/**
 * A <tt>DEFAULT FAMILY (family)</tt> clause in a field mapping; all fields that are not
 * explicitly used elsewhere in the field mapping are mapped to <tt>familyName:fieldName</tt>.
 *
 * @param familyName the name of the column family where target fields should be written.
 */
class DefaultFamilyMapping(val familyName: String) extends FieldMappingElem {
  /** {@inheritDoc} */
  override def equals(other: Any): Boolean = {
    other match {
      case default: DefaultFamilyMapping => {
        return familyName == default.familyName
      }
      case _ => { return false }
    }
  }

  /** {@inheritDoc} */
  override def hashCode(): Int = {
    return familyName.hashCode()
  }
}

/**
 * A <tt>field =&gt; $ENTITY</tt> clause, specifying that the named field is to be used
 * as the target entity id during the import.
 *
 * @param fieldName the name of the field to use as the entity id.
 */
class EntityFieldMapping(val fieldName: String) extends FieldMappingElem {
  /** {@inheritDoc} */
  override def equals(other: Any): Boolean = {
    other match {
      case entity: EntityFieldMapping => {
        return fieldName == entity.fieldName
      }
      case _ => { return false }
    }
  }

  /** {@inheritDoc} */
  override def hashCode(): Int = {
    return fieldName.hashCode()
  }
}

/**
 * A <tt>field =&gt; $TIMESTAMP</tt> clause, specifying that the named field is to be used
 * as the timestamp for the imported record during the import.
 *
 * @param fieldName the name of the field to use as the timestamp.
 */
class TimestampFieldMapping(val fieldName: String) extends FieldMappingElem {
  /** {@inheritDoc} */
  override def equals(other: Any): Boolean = {
    other match {
      case timestamp: TimestampFieldMapping => {
        return fieldName == timestamp.fieldName
      }
      case _ => { return false }
    }
  }

  /** {@inheritDoc} */
  override def hashCode(): Int = {
    return fieldName.hashCode()
  }
}
