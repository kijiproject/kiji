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

package org.kiji.express.modeling.config

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.KijiColumnRangeFilter

/**
 * An Express column filter based on the given minimum and maximum qualifier bounds.
 *
 * @param minQualifier is the minimum qualifier bound; null means none.
 * @param minIncluded if the lower bound should be included.
 * @param maxQualifier is the maximum qualifier bound; null means none.
 * @param maxIncluded if the upper bound should be included.
 */
@ApiAudience.Public
@ApiStability.Experimental
case class ColumnRangeFilter(minQualifier: String, minIncluded: Boolean, maxQualifier: String,
    maxIncluded: Boolean) extends ExpressColumnFilter {
  /**
   * Returns a concrete KijiColumnFilter that implements this express column filter.
   */
  def getKijiColumnFilter(): KijiColumnFilter = new KijiColumnRangeFilter(minQualifier, minIncluded,
      maxQualifier, maxIncluded)
}
