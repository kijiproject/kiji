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

package org.kiji.schema.shell.ddl

import org.kiji.annotations.ApiAudience

/**
 * Handy string formatting utility functions used by DDLCommands.
 */
@ApiAudience.Private
trait StrFormatting {
  /**
   * Format a string padded with right-justified space (' ') characters to the
   * 'len' limit, for use when printing formatted tables.
   *
   * <p>More formally, this function returns `str` with 0 or more space
   * characters appended to it; the number of space characters appended is
   * `len - str.length`. This method will not truncate `str`.
   * </p>
   *
   * <p>e.g., to print the following table:</p>
   * <div><pre>`
   * FOO  BAR
   * f1   b1
   * f2   b2
   * `</pre></div>, use:
   *
   * <div><pre>`val fLen = 5
   * val out = new StringBuilder
   * out.append(padTo("FOO", fLen).append("BAR\n")
   *    .append(padTo("f1", fLen)).append("b1\n")
   *    .append(padTo("f2", fLen)).append("b2\n")
   * `</pre></div>
   *
   * @param str the string to return in padded form; must not be null.
   * @param len the maximum expected width of any value for `str`.
   * @return the padded string.
   */
  final protected def padTo(str: String, len: Int): String = {
    require(len > 0, "Must have positive line length")
    return str + " " * (len - str.length)
  }
}
