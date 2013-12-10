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

package org.kiji.express.examples

import scala.util.Random

import com.twitter.scalding.Args
import org.apache.avro.Schema

import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiJob
import org.kiji.express.flow.KijiOutput
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.SchemaSpec.Generic

/**
 * NewsgroupSegmenter segments the rows of a table according to a specified ratio.  This is used
 * to segment the rows of a table into train and test sets.  By default, it segments into train and
 * test sets in the ratio 9:1.
 *
 * Example usage:
 *   express job /path/to/this/jar org.kiji.express.examples.NewsgroupSegmenter \
 *       --table kiji://.env/default/postings \
 *       --trainToTestRatio 9
 *
 * @param args to the job.  Specify `--table kiji://path/to/myTable` to specify the table this
 *     should be run on, and optionally `--trainToTestRatio 9` to specify the ratio of
 *     train rows to test rows.  By default the ratio is 9 train rows to 1 test row.
 */
class NewsgroupSegmenter(args: Args) extends KijiJob(args) {
  val tableURIString: String = args("table")
  val ratio: Int = args.getOrElse("trainToTestRatio", "9").toInt

  KijiInput(tableURIString, "info:group" -> 'group)
      .map(() -> 'segment) {
        _: Unit => if (Random.nextInt(ratio + 1) >= 1) 1 else 0
      }
      .write(KijiOutput(tableURIString, Map('segment ->
          QualifiedColumnOutputSpec.builder
              .withColumn("info", "segment")
              .withSchemaSpec(Generic(Schema.create(Schema.Type.INT)))
              .build)))
}
