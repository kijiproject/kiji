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

package org.kiji.chopsticks

import scala.actors.Futures._
import scala.io.Source
import java.io.File

import org.kiji.chopsticks.Resources._
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import org.kiji.schema.util.ResourceUtils

/**
 * <p>Reads the 20-newsgroup data set and parses each post into individual words. Each word is then
 * written to the column "info:word" in the row with entity id equivalent to the word, in the Kiji
 * table "words".</p>
 *
 * <p>This loader can be run from a command line shell as follows:
 * <code>
 *   chop jar <path/to/this/jar> org.kiji.chopsticks.NewsgroupLoader \
 *       <kiji://uri/to/kiji/table> <path/to/newsgroups/root/>
 * </code>
 * </p>
 *
 * <p>The Kiji table "words" must be created before this loader is run. This can be done with the
 * following KijiSchema DDL Shell command:
 * <code>
 *   words
 *     CREATE TABLE words WITH DESCRIPTION 'Words in the 20Newsgroups dataset.'
 *     ROW KEY FORMAT HASHED
 *     WITH LOCALITY GROUP default WITH DESCRIPTION 'Main storage.' (
 *       MAXVERSIONS = 1,
 *       TTL = FOREVER,
 *       COMPRESSED WITH GZIP,
 *       FAMILY info WITH DESCRIPTION 'Basic information' (
 *         word "string" WITH DESCRIPTION 'The word.'
 *       )
 *     );
 * </code>
 * </p>
 */
object NewsgroupLoader {
  /**
   * Reads a newsgroup post from a file and returns the sequence of words contained in the post.
   *
   * @param posting file from which the newsgroup post will be read.
   * @return the sequence of words that appear in the post read, or the empty sequence if there is
   *     a problem reading the file.
   */
  def getWords(posting: File): Seq[String] = {
    println("Reading file '%s'".format(posting.getPath()))
    var postSource: Option[Source] = None;
    try {
      postSource = Some(Source.fromFile(posting.getPath()))
      postSource.get.mkString.split("""\s+""")
    } catch {
      case e: Exception => {
        Console.err.println("Ignored file %s due to Exception: %s"
            .format(posting.getPath(), e.getMessage))
        e.printStackTrace(Console.err)
        return Seq()
      }
    } finally {
      postSource.foreach { _.close() }
    }
  }

  /**
   * Runs the loader.
   *
   * @param args passed in from the command line.
   */
  def main(args: Array[String]) {
    // Read in command line arguments.
    val uri = KijiURI.newBuilder(args(0)).build()
    val root = new File(args(1))

    if (!root.isDirectory()) {
      sys.error("Newsgroup root must be a folder (was: %s)".format(root.getPath()))
    }

    // Parse the postings.
    doAndRelease { Kiji.Factory.open(uri) } { kiji: Kiji =>
      val tasks = for {
        newsgroup <- root.listFiles()
      } yield future {
        doAndRelease { kiji.openTable("words") } { table: KijiTable =>
          doAndClose { table.openTableWriter() } { writer: KijiTableWriter =>
            for {
              posting <- newsgroup.listFiles()
              word <- getWords(posting)
            } writer.put(table.getEntityId(word), "info", "word", word)
          }
        }
      }

      tasks.foreach { _() }
    }
  }
}
