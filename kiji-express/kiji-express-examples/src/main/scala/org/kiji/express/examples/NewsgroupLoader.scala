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

import scala.actors.Futures.future
import scala.io.Source

import org.kiji.express.flow.util.ResourceUtil._
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI

/**
 * <p>
 *   Reads the 20Newsgroups data set and writes each post's contents to the info:post column. The
 *   info:group column also will get populated with the name of the newsgroup that the post belongs
 *   to.
 * </p>
 *
 * <p>
 *   This loader can be run from a command line shell as follows:
 *   <code>
 *     express jar <path/to/this/jar> org.kiji.express.examples.NewsgroupLoader \
 *         <kiji://uri/to/kiji/table> <path/to/newsgroups/root/>
 *   </code>
 * </p>
 *
 * <p>
 *   The Kiji table "postings" must be created before this loader is run. This can be done with the
 *   following KijiSchema DDL Shell command in src/main/ddl/postings.ddl.
 * </p>
 */
object NewsgroupLoader {
  /**
   * Runs the loader.
   *
   * @param args passed in from the command line.
   */
  def main(args: Array[String]) {
    // Read in command line arguments.
    val uri = KijiURI.newBuilder(args(0)).build()
    val root = new File(args(1))
    require(root.isDirectory, "Newsgroup root must be a folder (was: %s)".format(root.getPath))

    doAndRelease { Kiji.Factory.open(uri) } { kiji: Kiji =>
      val tasks = root
          .listFiles()
          .map { newsgroup: File =>
            // Build of a series of tasks for loading each newsgroup.
            future {
              // Open a KijiTableWriter.
              doAndRelease { kiji.openTable(uri.getTable) } { table: KijiTable =>
                doAndClose { table.openTableWriter() } { writer: KijiTableWriter =>
                  newsgroup
                      .listFiles()
                      .foreach { posting: File =>
                        // Get the post's contents.
                        val text = doAndClose { Source.fromFile(posting) } { source: Source =>
                          source.mkString
                        }

                        // Write the post to the table.
                        val entityId = table.getEntityId(newsgroup.getName, posting.getName)
                        writer.put(entityId, "info", "post", text)
                        writer.put(entityId, "info", "group", newsgroup.getName)
                      }
                }
              }
            }
          }

      // Executes each task using a thread pool. For more information about scala futures see:
      // http://docs.scala-lang.org/overviews/core/futures.html
      tasks.foreach { task => task.apply() }
    }
  }
}
