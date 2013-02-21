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

package org.kiji.mapreduce.lib.util;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;

/**
 * Parser that extracts fields from the NCSA Common Log Format.  The fields available for
 * extraction are represented by the enumeration {@link Field}.  No conversion is done for any
 * fields which might have a type associated (such as sizes, codes, or dates).
 *
 * <h2>Typical invocation:</h2>
 * <pre><code>
 *   Map&lt;Field, String&gt; derivedFields = CommonLogParser.get().parseCommonLog(logStr);
 * </code></pre>
 *
 * @see <a href="http://www.w3.org/Daemon/User/Config/Logging.html#common-logfile-format">
 *     Common logfile format</a>
 */
@ApiAudience.Public
public final class CommonLogParser {

  /** Fields that can be extracted via the Common Log Format. */
  public static enum Field {
    REMOTEHOST,
    IDENT,
    AUTHUSER,
    DATE,
    REQUEST,
    STATUS,
    BYTES
  }

  /** Private constructor to prevent instantiation. Use get() to get an instance of this. */
  private CommonLogParser() { }

  private static final CommonLogParser SINGLETON = new CommonLogParser();

  /** @return an instance of CommonLogParser. */
  public static CommonLogParser get() {
    return SINGLETON;
  }

  /**
   * Parses the input text with the specified delimiter.
   *
   * @param line of text to parse the individual fields from.
   * @return list of strings for the parsed fields.
   * @throws ParseException if there is an issue with escaping, or if there are insufficient fields
   * @throws IllegalArgumentException if the line is null.
   */
  public Map<Field, String> parseCommonLog(String line)
      throws ParseException {
    Preconditions.checkNotNull(line);

    if (line.isEmpty()) {
      throw new ParseException("Could not parse empty line.", 0);
    }

    // Processed character count for better exceptions.
    int processed = 0;
    Map<Field, String> derivedFields = Maps.newHashMap();

    List<String> tokens = Arrays.asList(line.split(" "));
    Iterator<String> tokenItr = tokens.iterator();

    String remotehost = tokenItr.next();
    derivedFields.put(Field.REMOTEHOST, remotehost);
    processed += remotehost.length();

    if (!tokenItr.hasNext()) {
      throw new ParseException("Not enough tokens to parse ident.", processed);
    }
    String ident = tokenItr.next();
    derivedFields.put(Field.IDENT, ident);
    processed += ident.length() + 1;

    if (!tokenItr.hasNext()) {
      throw new ParseException("Not enough tokens to parse authuser.", processed);
    }
    String authuser = tokenItr.next();
    derivedFields.put(Field.AUTHUSER, authuser);
    processed += authuser.length() + 1;

    if (!tokenItr.hasNext()) {
      throw new ParseException("Not enough tokens to parse date.", processed);
    }
    String token = tokenItr.next();
    StringBuilder sb = new StringBuilder().append(token);
    if (token.charAt(0) == '[') {
      while (token.charAt(token.length() - 1) != ']') {
        processed += token.length() + 1;
        if (!tokenItr.hasNext()) {
          throw new ParseException("Not enough tokens to parse date.", processed);
        }
        token = tokenItr.next();
        sb.append(" ").append(token);
      }
    } else {
      throw new ParseException("Date field needs to begin with a [", processed + 2);
    }
    processed += token.length() + 1;
    String date = sb.toString();
    // Trim [] from the derived field.
    derivedFields.put(Field.DATE, date.substring(1, date.length() - 1));

    if (!tokenItr.hasNext()) {
      throw new ParseException("Not enough tokens to parse request.", processed);
    }
    token = tokenItr.next();
    sb = new StringBuilder().append(token);
    if (token.charAt(0) == '"') {
      while (token.charAt(token.length() - 1) != '\"') {
        processed += token.length() + 1;
        if (!tokenItr.hasNext()) {
          throw new ParseException("Not enough tokens to parse request.", processed);
        }
        token = tokenItr.next();
        sb.append(" ").append(token);
      }
    } else {
      throw new ParseException("Request field needs to begin with a \"", processed + 2);
    }
    processed += token.length() + 1;
    String request = sb.toString();
    // Trim "" from the derived field
    derivedFields.put(Field.REQUEST, request.substring(1, request.length() - 1));

    if (!tokenItr.hasNext()) {
      throw new ParseException("Not enough tokens to parse status.", processed);
    }
    String status = tokenItr.next();
    derivedFields.put(Field.STATUS, status);
    processed += status.length() + 1;

    if (!tokenItr.hasNext()) {
      throw new ParseException("Not enough tokens to parse bytes.", processed);
    }
    String bytes = tokenItr.next();
    derivedFields.put(Field.BYTES, bytes);

    return derivedFields;
  }

}
