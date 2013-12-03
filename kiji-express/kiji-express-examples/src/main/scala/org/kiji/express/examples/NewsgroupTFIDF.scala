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

import com.twitter.scalding.Args
import com.twitter.scalding.Tsv

import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiJob

/**
 * This job calculates, for every word in the training set, the number of posts it appears in in
 * each category divided by the number of posts it appears in in all categories.  The output of
 * this job defines the trained parameters for our naive bayes classification model,
 * which is evaluated in [[org.kiji.express.examples.NewsgroupClassifier]].
 *
 * Example usage:
 *    express job /path/to/this/jar org.kiji.express.examples.NewsgroupTFIDF \
 *    --table kiji://.env/default/postings \
 *    --out-file /path/to/tfidf/file
 *
 * The out-file will be used as input to the next phase, so remember the location.
 *
 * @param args to the job. `--table kiji://path/to/myTable` specifies the table to run on.
 *     `--out-file file://path/to/outputFile` specifies the path to the output file,
 *     which will be a Tsv.
 */
class NewsgroupTFIDF(args: Args) extends KijiJob(args) {
  val tableURIString: String = args("table")
  val outFile: String = args("out-file")

  // Calculates the number of posts which contain each word regardless of category.
  val df = KijiInput(tableURIString, "info:segment" -> 'segment, "info:post" -> 'postText)
      // Include only items from the training segment.
      .filter('segment) {
        segment: Seq[FlowCell[Int]] => segment.head.datum == 1
      }
      // Create unique tokens from the post text and create a new tuple for each one.
      .flatMapTo('postText -> 'word) {
        postText: Seq[FlowCell[CharSequence]] =>
            NewsgroupTFIDF.uniquelyTokenize(postText.head.datum)
      }
      // Get the number of posts across all groups which contain each word.
      .groupBy('word) {
        _.size('documentFrequency)
      }

  // Calculates the number of posts within each category which contain each word.
  val tf = KijiInput(tableURIString, "info:segment" -> 'segment, "info:post" -> 'postText)
      // Include only items from the training segment.
      .filter('segment) {
        segment: Seq[FlowCell[Int]] => segment.head.datum == 1
      }
      // Extract the group field from the entityId.
      .map('entityId -> 'group) { entityId: EntityId => entityId(0) }
      // Create unique tokens from the post text and create a new tuple for each one.
      .flatMap('postText -> 'word) {
        postText: Seq[FlowCell[CharSequence]] =>
            NewsgroupTFIDF.uniquelyTokenize(postText.head.datum)
      }
      // Get the number of posts in each category which contain each word.
      .groupBy('group, 'word) { _.size('termFrequency) }

  tf.joinWithSmaller('word -> 'word, df)
      // Calculate the weight of each word for each category.
      .map(('termFrequency, 'documentFrequency) -> 'weight) {
        tfdf: (Long, Long) => {
          val (tf, df) = tfdf
          tf.toDouble / df.toDouble
        }
      }
      .write(Tsv(outFile, writeHeader = true))
}

object NewsgroupTFIDF {
  /**
   * Collect the string tokens from a newsgroup post.
   *
   * @param post is a newsgroup post to tokenize.
   * @return an iterable of the string tokens contained in the post.
   */
  def tokenize(post: CharSequence): Seq[String] = {
    // Regular expression for matching words. For more information see:
    // http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html.
    val wordRegex = """\b\p{L}+\b""".r

    // Split the text up into words.
    wordRegex
        .findAllIn(post)
        .map { word: String => word.toLowerCase }
        .toSeq
  }

  /**
   * Collect the unique tokens from a newsgroup post.
   *
   * @param post is a newsgroup post to tokenize.
   * @return a set of string tokens contained in the post.
   */
  def uniquelyTokenize(post: CharSequence): Set[String] = {
    tokenize(post).toSet
  }
}
