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

import java.io.File

import com.twitter.scalding.Args
import com.twitter.scalding.Tsv

import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiJob

/**
 * This job runs a naive bayes classifier using the output from
 * [[org.kiji.express.examples.NewsgroupTFIDF]] on each post in the testing set, compares the result
 * of the classifier with the known label, and reports the overall accuracy of the classifier.
 *
 * Example usage:
 *    express job /path/to/this/jar org.kiji.express.examples.NewsgroupClassifier \
 *    --table kiji://.env/default/postings \
 *    --data-root /path/to/dataset/root/directory/ \
 *    --weights-file /path/to/tfidf/weights/file \
 *    --out-file /path/to/percent/correct/file
 *
 * --weights-file should be the same as --out-file of [[org.kiji.express.examples.NewsgroupTFIDF]]
 *
 * @param args to the job. `--table kiji://path/to/myTable` specifies the table to run on (where
 *     the training set is located. `--out-file file://path/to/my-output` specifies the file to
 *     write the overall accuracy to. `--data-root file://path/to/20Newsgroups/rootDir` specifies
 *     the root directory of the 20Newsgroups project, which will be used to gather a list of all
 *     possible labels (the group names) from the directory structure.
 */
class NewsgroupClassifier(args: Args) extends KijiJob(args) {
  val tableURIString: String = args("table")
  val outFile: String = args("out-file")
  val categories: Set[String] = new File(args("data-root")).listFiles().map { _.getName }.toSet
  val weights = Tsv(
      args("weights-file"),
      ('group, 'word, 'tf, 'df, 'weight),
      skipHeader = true)
    .discard('tf, 'df)

  // Classifies the test posts based on weights calculated above and returns the percent of those
  // which matched their tagged group.
  KijiInput(tableURIString,
      "info:segment" -> 'segment,
      "info:post" -> 'postText,
      "info:group" -> 'tag)
    // Include only items from the test segment.
    .filter('segment) {
      segment: Seq[FlowCell[Int]] => segment.head.datum == 0
    }
    // Create a tuple for every possible group.
    .flatMap(() -> 'maybe) {
      _: Unit => categories
    }
    // Create unique tokens from the post text and create a new tuple for each one.
    .flatMap('postText -> 'word) {
      postText: Seq[FlowCell[CharSequence]] => NewsgroupTFIDF.uniquelyTokenize(postText.head.datum)
    }
    .map('tag -> 'tag) { tag: Seq[FlowCell[CharSequence]] => tag.head.datum.toString }
    // Pull in the precalculated weights for each word, group pair.
    .joinWithTiny(('word, 'maybe) -> ('word, 'group), weights)
    // Take the product of all weights within a group for each post.
    .groupBy('entityId, 'group, 'tag) {
      _.reduce('weight -> 'groupWeight) {
        (w1: Double, w2: Double) => w1 * w2
      }
    }
    // Collect the highest weighted prediction.
    .groupBy('entityId, 'tag) {
      _.reduce(('group, 'groupWeight) -> ('bestGuess, 'bestGuessWeight)) {
        (tuple: (_, Double), tuple2: (_, Double)) => {
          val (_, firstWeight) = tuple
          val (_, secondWeight) = tuple2
          if (firstWeight > secondWeight) tuple else tuple2
        }
      }
    }
    // If the tag is the same as our best guess, emit a 1, otherwise emit a 0.
    .map(('tag, 'bestGuess) -> 'correct) {
      tuple: (String, String) => {
        val (tag, bestGuess) = tuple
        if (tag.equals(bestGuess)) 1 else 0
      }
    }
    // Sum the correct guesses and the total posts classified.
    .groupAll {
      _.foldLeft('correct -> ('totalCorrect, 'total))((0, 0)) {
        (accumulator: (Int, Int), correct: Int) =>{
          val (totalCorrect, total) = accumulator
          (totalCorrect + correct, total + 1)
        }
      }
    }
    // Calculate the percent correct.
    .map(('totalCorrect, 'total) -> 'percentCorrect) {
      tuple: (Int, Int) => {
        val (totalCorrect, total) = tuple
        totalCorrect.toDouble / total.toDouble
      }
    }
    .write(Tsv(outFile, writeHeader = true))
}
