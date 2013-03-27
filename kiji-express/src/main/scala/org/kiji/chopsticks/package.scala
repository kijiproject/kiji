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

package org.kiji

/**
 * KijiChopsticks is a domain-specific language for analyzing and modeling data stored in Kiji.
 *
 * For more information on Kiji, please visit [[http://www.kiji.org]] and the Kiji Github page at
 * [[https://github.com/kijiproject]]. KijiChopsticks is built atop Scalding
 * ([[https://github.com/twitter/scalding]]), a library for writing MapReduce workflows.
 *
 * === Getting started. ===
 * To use KijiChopsticks, import the Scalding library, the `chopsticks` library, and members of the
 * object [[org.kiji.chopsticks.DSL]].
 * {{{
 *   import com.twitter.scalding._
 *   import org.kiji.chopsticks._
 *   import org.kiji.chopsticks.DSL._
 * }}}
 * Doing so will import several classes and functions that make it easy to author analysis
 * pipelines.
 *
 * === Working with data from a Kiji table. ===
 * Scalding represents distributed data sets as a collection of tuples with named fields.
 * Likewise, KijiChopsticks represents a row in a Kiji table as a tuple.
 * [[org.kiji.chopsticks.DSL]] provides several factory methods, named `KijiInput`,
 * that make it easy to specify what columns you want to read from a Kiji table and what names
 * they should have in row tuples. For example, to read the value of a column named `info:text`
 * from a table named `postings` into the tuple field named `text`, you could write the following.
 *
 * {{{
 *   KijiInput("kiji://.env/default/postings")("info:text" -> "text")
 * }}}
 *
 * The result of the above expression is a [[org.kiji.chopsticks.KijiSource]] (an implementation
 * of Scalding's `Source`) which represents the rows of the Kiji table as a collection of tuples.
 * At this point, functional operators can be used to manipulate and analyze the data. For example,
 * to split the contents of the column `info:text` into individual words,
 * you could write the following.
 * {{{
 *   KijiInput("kiji://.env/default/postings")("info:text" -> 'text)
 *       .flatMap('text -> 'word) { word: NavigableMap[Long, Utf8] =>
 *         getMostRecent(word)
 *             .toString()
 *             .split("""\s+""")
 *       }
 * }}}
 */
package object chopsticks
