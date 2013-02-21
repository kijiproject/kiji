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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.kiji.annotations.ApiAudience;

/**
 * Parser that extracts fields from RFC 4180 (http://tools.ietf.org/html/rfc4180) compliant
 * CSV and TSV lines of text.
 *
 * <h2>Typical invocations to parse CSV and TSV lines respectively:</h2>
 * <pre><code>
 *   List&lt;String&gt; parsedoCSVFields = CSVParser.parseCSV("first,last");
 *
 *   List&lt;String&gt; parsedTSVFields TSVParser.parseTSV("first\tlast");
 * </code></pre>
 *
 * <p>The difference between these methods and String.split(',') is that this handles the escaping
 * of double quotes in the manner specified by RFC 4180 Section 2.7.</p>
 */
@ApiAudience.Public
public final class CSVParser {
  private static final String CSV_DELIMITER = ",";
  private static final String TSV_DELIMITER = "\t";

  // RFC 4180 uses double quotes for the purposes of escaping.
  private static final char ESCAPE_CHARACTER = '"';

  // Precompiled patterns for splitting CSVs and TSVs.
  private static final Pattern CSV_PATTERN = Pattern.compile(CSV_DELIMITER);
  private static final Pattern TSV_PATTERN = Pattern.compile(TSV_DELIMITER);

  /** Private constructor to prevent instantiation. */
  private CSVParser() { }

  /**
   * Parses the input text using comma as the delimiter.
   *
   * @param line of text to parse the individual fields from.
   * @return list of strings for each of the parsed fields (using comma as a delimiter).
   * @throws ParseException if there is an issue with escaping.
   */
  public static List<String> parseCSV(String line) throws ParseException {
    return parseFields(line, CSV_PATTERN);
  }

  /**
   * Parses the input text using tab as the delimiter.
   *
   * @param line of text to parse the individual fields from.
   * @return list of strings for each of the parsed fields (using tab as a delimiter).
   * @throws ParseException if there is an issue with escaping.
   */
  public static List<String> parseTSV(String line) throws ParseException {
    return parseFields(line, TSV_PATTERN);
  }

  /**
   * Parses the input text with the specified delimiter.
   *
   * @param line of text to parse the individual fields from.
   * @param pattern the pattern used to separate the fields.
   * @return list of strings for the parsed fields.
   * @throws ParseException if there is an issue with escaping.
   */
  private static List<String> parseFields(String line, Pattern pattern)
      throws ParseException {
    List<String> derivedFields = new ArrayList();
    // -1 limit parameter is used to keep trailing fields in the result.
    List<String> tokens = Arrays.asList(pattern.split(line, -1));
    Iterator<String> tokenItr = tokens.iterator();

    while (tokenItr.hasNext()) {
      String token = tokenItr.next();

      // Escaped strings must comprise the entire field
      if (token.indexOf(ESCAPE_CHARACTER) > 0) {
        throw new ParseException("Optional double quotes(\") not at the beginning of the field",
            line.indexOf(ESCAPE_CHARACTER));
      }
      // If this is an escaped string, parse individual characters to handle escaping
      if (token.length() != 0 && token.charAt(0) == ESCAPE_CHARACTER) {
        StringBuilder sb = new StringBuilder();
        int pos = 1; // Start beyond the quote
        boolean done = false;
        while (!done) {
          char c = token.charAt(pos);
          // According to the specification laid out in section 2.7 of RFC 4180: fields can be
          // enclosed in double quotes.  If double quotes appear inside of a field, it must be
          // escaped by preceeding it with another double quote.
          if (c == ESCAPE_CHARACTER) {
            if (pos == token.length() - 1) {
              // If a single escape character(") is at the end, then we are done!
              done = true;
            } else if (token.charAt(pos + 1) == ESCAPE_CHARACTER) {
              // Two of the escape character(") in sequence, then add and advance twice.
              sb.append(ESCAPE_CHARACTER);
              pos += 2;
            } else {
              // Unescaped escape characters not at the end are a parse exception.
              throw new ParseException("Stray double quote",  pos);
            }
          } else {
            sb.append(c);
            pos++;
          }

          // If we hit the end of this token and the escaped string isn't over,
          // parse the next token into this string.
          if (pos == token.length()) {
            if (tokenItr.hasNext()) {
              sb.append(pattern.pattern());
              token = tokenItr.next();
              pos = 0;
            } else {
              // No next token, so clearly this string never got unescaped
              throw new ParseException("Unmatched double quote:" + sb.toString(), line.length());
            }
          }
        }

        // We are done, so build this token to put into the derived fields list.
        token = sb.toString();
      }
      derivedFields.add(token);
    }
    return derivedFields;
  }

}
