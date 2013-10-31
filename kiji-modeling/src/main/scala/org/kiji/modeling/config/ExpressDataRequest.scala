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

package org.kiji.modeling.config

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest

/**
 * Encapsulates parameters for a data request.
 *
 * @param minTimestamp to accept for cells.
 * @param maxTimestamp to accept for cells.
 * @param columnRequests for required columns.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ExpressDataRequest(
    minTimestamp: Long,
    maxTimestamp: Long,
    columnRequests: Seq[ExpressColumnRequest]) {
  /**
   * Converts this Express data request to a KijiDataRequest.
   *
   * @return the requested Kiji data request.
   */
  private[modeling] def toKijiDataRequest: KijiDataRequest = {
    val builder = KijiDataRequest.builder().withTimeRange(minTimestamp, maxTimestamp)
    columnRequests.foreach { colRequest: ExpressColumnRequest =>
      if (colRequest.filter.isDefined) {
        builder.newColumnsDef()
            .withMaxVersions(colRequest.maxVersions)
            .withFilter(colRequest.filter.get.toKijiColumnFilter)
            .add(new KijiColumnName(colRequest.name))
      } else {
        builder.newColumnsDef()
            .withMaxVersions(colRequest.maxVersions)
            .add(new KijiColumnName(colRequest.name))
      }
    }
    builder.build
  }
}

/**
 * Encapsulates parameters for a request of a particular column.
 *
 * @param name for the column. For example, "info:name".
 * @param maxVersions to be returned from the column, for a given entity.
 * @param filter to apply to this column.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ExpressColumnRequest(
    name: String,
    maxVersions: Int,
    filter: Option[ExpressColumnFilter])
