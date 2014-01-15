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

package org.kiji.modeling.examples.ItemItemCF

import com.twitter.scalding.Args
import com.twitter.scalding.TextLine
import cascading.pipe.Pipe

import org.kiji.express._
import org.kiji.express.flow._

/**
 * Populates a table of movie ratings.
 *
 * Reads in a file with records of the form: `user`, `movie`, `rating`.
 *
 * @param args passed in from the command line.
 */
class MovieImporter(args: Args) extends KijiJob(args) {
  // Get user ratings
  TextLine(args("ratings"))
      .read
      .mapTo('line -> ('user, 'movie, 'rating)) {
        line: String => {
          val contents: Array[String] = line.split("\t")
          // Cast the user and movie into longs, rating into double
          (contents(0).toLong, contents(1).toLong, contents(2).toDouble)
        }
      }

      // Mark the movieId as the entityId
      .map('user -> 'entityId) { user: Long => EntityId(user) }
      .write(KijiOutput.builder
          .withTableURI(args("table-uri"))
          .withColumnSpecs(Map(
              'rating -> ColumnFamilyOutputSpec.builder
                  .withFamily("ratings")
                  .withQualifierSelector('movie)
                  .build))
          .build)
}
