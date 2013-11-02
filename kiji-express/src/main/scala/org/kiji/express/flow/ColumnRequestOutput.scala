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

package org.kiji.express.flow

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException

/** Convenience class for specifying the schema to use for writes. */
private[express] case class WriterSchemaSpec (
    useDefaultReader: Boolean = false,
    schemaId: Option[Long] = None) {
  require(useDefaultReader || schemaId.isDefined)
}

/**
 * Interface for all (group- and map-type) ColumnRequestOutput objects.
 *
 * Except for ``getColumnName,`` The entire API is read-only (outside of [[org.kiji.express]]).  We
 * assume that users will specify output requests using the factory methods ``object
 * ColumnRequestOutput.``
 *
 * Note that the subclasses of ColumnRequestOutput are case classes, and so they override
 * ColumnRequestOutput's abstract methods (e.g., ``schemaId``) with vals.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
trait ColumnRequestOutput {

  /** Specifies the schema to use during a write (default is ``None``). */
  private[express] def schemaId: Option[Long]

  /** Indicates whether to use the default reader schema to write. */
  private[express] def useDefaultReaderSchema: Boolean

  /** Legacy method to get the writer schema. */
  private[express] def writerSchemaSpec: Option[WriterSchemaSpec] =
    (useDefaultReaderSchema, schemaId) match {
      case (false, None) => None
      case _ => Some(WriterSchemaSpec(useDefaultReaderSchema, schemaId))
    }

  /**
   * Creates a copy of this column request with the writer schema set up for testing.
   *
   * Used within Express for testing infrastructure.
   */
  private[express] def newGetAllData: ColumnRequestOutput

  /**
   * Returns the standard KijiColumnName representation of the name of the column this
   * ColumnRequests is for.
   *
   * @return the name of the column this ColumnRequest specifies.
   */
  private[express] def getColumnName: KijiColumnName
}

/**
 * Specification for writing to a qualified column in a Kiji table.
 *
 * Note that you must specify a ``schemaId`` or set ``useDefaultReaderSchema=true`` or else
 * [[org.kiji.express.flow.framework.KijiScheme]] will raise a ``InvalidKijiTapException`` (unless
 * you are writing a test case, in which case not specifying a scheme is okay).
 *
 * @param family The requested column family name.
 * @param qualifier The requested column qualifier name.
 * @param schemaId The specific schema for writes (default is ``None``).
 * @param useDefaultReaderSchema Use the default reader schema during a write (default is false).
 */
final case class QualifiedColumnRequestOutput (
    val family: String,
    val qualifier: String,
    val schemaId: Option[Long] = None,
    val useDefaultReaderSchema: Boolean = false
) extends ColumnRequestOutput {
  // TODO: Require a schema (EXP-248)
  //require(useDefaultReaderSchema || schemaId.isDefined)

  /** Returns name (``family:qualifier``) for requested column as KijiColumnName. */
  override def getColumnName(): KijiColumnName = new KijiColumnName(family, qualifier)

  /**
   * Creates a copy of this column request with the writer schema set up for testing.
   *
   * Used within Express for testing infrastructure.
   */
  private[express] def newGetAllData: QualifiedColumnRequestOutput = {
    writerSchemaSpec match {
      case Some(spec) =>
        if (spec.useDefaultReader) {
          copy(useDefaultReaderSchema = true)
        } else {
          copy(schemaId=Some(spec.schemaId.get))
        }
      case None => this
    }
  }
}

/**
 * Specification for writing to a map-type column family in a Kiji table.
 *
 * Note that you must specify a ``schemaId`` or set ``useDefaultReaderSchema=true`` or else
 * [[org.kiji.express.flow.framework.KijiScheme]] will raise a ``InvalidKijiTapException`` (unless
 * you are writing a test case, in which case not specifying a scheme is okay).
 *
 * @param family The requested column family name.
 * @param qualifierSelector The qualifier to write to in the specified map-type family.
 * @param schemaId The specific schema for writes (default is ``None``).
 * @param useDefaultReaderSchema Use the default reader schema during a write (default is false).
 */
final case class ColumnFamilyRequestOutput (
    val family: String,
    val qualifierSelector: String,
    val schemaId: Option[Long] = None,
    val useDefaultReaderSchema: Boolean = false
) extends ColumnRequestOutput {
  // TODO: Require a schema (EXP-248)
  //require(useDefaultReaderSchema || schemaId.isDefined)
  if (family.contains(':')) {
    throw new KijiInvalidNameException("Cannot have a : in family name for map-type request")
  }

  /** Returns name (``family``) for requested column as KijiColumnName. */
  override def getColumnName(): KijiColumnName = new KijiColumnName(family)

  /**
   * Creates a copy of this column request with the writer schema set up for testing.
   *
   * Used within Express for testing infrastructure.
   */
  private[express] def newGetAllData: ColumnFamilyRequestOutput = {
    writerSchemaSpec match {
      case Some(spec) =>
        if (spec.useDefaultReader) {
          copy(useDefaultReaderSchema = true)
        } else {
          copy(schemaId=Some(spec.schemaId.get))
        }
      case None => this
    }
  }
}

/** Factory object for creating group- and map-type column output requests. */
@ApiAudience.Public
@ApiStability.Experimental
object ColumnRequestOutput {
  /**
   * Factory method for ``QualifiedColumnRequestOutput`` and ``ColumnFamilyRequestOutput``.
   *
   * Will choose to create a group or map-type request appropriately depending on whether the
   * specified ``columnName`` contains a ``:`` or not.
   *
   * Note that you must specify a ``schemaId`` or set ``useDefaultReaderSchema=true`` or else
   * [[org.kiji.express.flow.framework.KijiScheme]] will raise a ``InvalidKijiTapException`` (unless
   * you are writing a test case, in which case not specifying a scheme is okay).
   *
   * @param columnName name of the requested column.  Requests for group-type columns should be of
   *     the form ``family:qualifier``.  Requests for map-type columns should be of the form
   *     ``family``.
   * @param schemaId The specific schema for writes (default is ``None``).
   * @param useDefaultReaderSchema Use the default reader schema during a write (default is false).
   * @param qualifierSelector The qualifier to write to if using a map-type family (if specified for
   *     a group-type family, will cause an ``IllegalArgumentException``.
   * @return A ``QualifiedColumnRequestOutput`` or ``ColumnFamilyRequestOutput`` object (depending
   *     on the presence of a ``:`` in ``columnName``) with its fields populated per the parameters.
   */
  def apply(
    columnName: String,
    schemaId: Option[Long] = None,
    useDefaultReaderSchema: Boolean = false,
    qualifierSelector: Option[String] = None
  ): ColumnRequestOutput = {
    val kijiColumn = new KijiColumnName(columnName)
    if (kijiColumn.isFullyQualified) {
      if (qualifierSelector.isDefined) {
        throw new IllegalArgumentException(
            "Cannot specify a qualifierSelector for a group-type column." )
      }
      new QualifiedColumnRequestOutput(
        family = kijiColumn.getFamily(),
        qualifier = kijiColumn.getQualifier(),
        schemaId = schemaId,
        useDefaultReaderSchema = useDefaultReaderSchema)
    } else {
      assert(qualifierSelector.isDefined)
      new ColumnFamilyRequestOutput(
        family = kijiColumn.getFamily(),
        schemaId = schemaId,
        useDefaultReaderSchema = useDefaultReaderSchema,
        qualifierSelector = qualifierSelector.get
      )
    }
  }
}
