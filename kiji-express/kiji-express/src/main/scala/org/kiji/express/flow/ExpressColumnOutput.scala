/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.kiji.express.flow.util.AvroUtil
import org.kiji.schema.KijiColumnName

/**
 * A class that holds information identifying a single qualified column in a Kiji table. An
 * object of this class is required for writing data via the TypeSafe API.
 *
 * The sink implementations for [[org.kiji.express.flow.framework.TypedKijiScheme]] and
 * [[org.kiji.express.flow.framework.TypedLocalKijiScheme]] expect the
 * [[com.twitter.scalding.typed.TypedPipe]] leading to the sink to hold an Iterable of the type
 * ExpressColumnOutput object:
 *
 * {{{
 *    val preExistingPipe: TypedPipe[EntityId, Long] = getPreExistingPipe()
 *
 *   //Writes values from a pre-existing typed pipe to two columns in kiji table.
 *   preExistingPipe.map{existingTuple: (EntityId, Long) =>
 *      val (entityId, value) = existingTuple
 *
 *      val col1: ExpressColumnOutput = ExpressColumnOutput(
 *          entityId,
 *          "myFamily",
 *          "myQualifier",
 *          value
 *      )
 *      val col2: ExpressColumnOutput = ExpressColumnOutput(
 *          entityId,
 *          "myFamily2",
 *          "myQalifier2",
 *          value * value,
 *          version = Some(1234l)
 *       )
 *
 *      //A list of ExpressColumnOutput's
 *      List(col1, col2)
 *   }.write(KijiOutput.typedSinkForTable("kiji://.env/mytable"))
 *
 * }}}
 *
 * @param entityId EntityId for the row.
 * @param family The family of the column being written to.
 * @param qualifier The qualifier of the column being written to.
 * @param datum The data to be written.
 * @param schemaSpec The schema specification with which to write data.
 * @param version The version for the data cell being written.
 * @tparam T The type of the datum that is being written to the table.
 */
@ApiAudience.Public
@ApiStability.Evolving
final class ExpressColumnOutput[T] private (
    val entityId: EntityId,
    val family: String,
    val qualifier: String,
    val datum: T,
    val schemaSpec: SchemaSpec,
    val version: Option[Long]
) {

  /**
   * Make a best effort to encode a provided value to a type that will be compatible with
   * the column. If no such conversion can be made, the original value will be returned.
   */
  private[express] def encode: Any => Any = {
    schemaSpec.schema.map(AvroUtil.avroEncoder).getOrElse(identity)
  }

  /**
   * The [[KijiColumnName]] for the column specified by this object.
   *
   * @return The KijiColumn name identifying the column being written to.
   */
  def columnName: KijiColumnName = KijiColumnName.create(family, qualifier)
}

/**
 * Companion object for ExpressColumnOutput.
 */
object ExpressColumnOutput {
  /**
   * Factory method to create an instance of ExpressColumnOutput.
   *
   * @param entityId EntityId for the row.
   * @param family The family of the column being written to.
   * @param qualifier The qualifier of the column being written to.
   * @param datum The data to be written.
   * @param schemaSpec The schema specification with which to write data.
   * @param version The version for the data cell being written.
   * @return A new instance of ExpressColumnOutput.
   */
  def apply[T](
      entityId: EntityId,
      family: String,
      qualifier: String,
      datum: T,
      schemaSpec: SchemaSpec = ColumnOutputSpec.DEFAULT_SCHEMA_SPEC,
      version: Option[Long] = None
  ): ExpressColumnOutput[T] = {

    new ExpressColumnOutput[T](
        entityId,
        family,
        qualifier,
        datum,
        schemaSpec,
        version)
  }
}
