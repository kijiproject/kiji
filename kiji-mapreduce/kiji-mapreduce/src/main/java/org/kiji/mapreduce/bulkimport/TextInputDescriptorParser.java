/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.bulkimport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * <p>This class is used by
 * {@link org.kiji.mapreduce.bulkimport.DescribedInputTextBulkImporter} to
 * parse input-descriptor files.
 * This input-descriptor file specifies the column qualifiers that parsed data is sent to.
 * It also specifies the writer schemas for serializing these columns.
 * Each column is represented in this file with an entry formated either as:</p>
 * <br/>qualifier:[qualifier_name]
 * <br/>schema:[schema_json_definition]
 * <br/>
 * <p>or as:</p>
 * <br/>skip-entry
 *
 * <p>In the first form, the qualifier must be exactly one line long, while the json
 * definition is free to span multiple lines.  The second form indicates that this column
 * should not be imported.  An empty line is used to separate entries.
 * See /examples/synthdata-input-descriptors.txt for an example input-descriptor file</p>
 *
 * <p>Use:</p>
 * <p>Parses an InputStream for a list of column names and schemas, and stores these in order.
 * Call parse(in) to parse column names and schema names to internal variables.
 * After this call, these variables may be recovered with <code>getQualifierInfo()</code>.</p>
 */
@ApiAudience.Private
public class TextInputDescriptorParser {
  private static final Logger LOG = LoggerFactory.getLogger(TextInputDescriptorParser.class);

  /** Token expected at the beginning of lines naming a qualifier. */
  public static final String QUALIFIER_TOKEN = "qualifier:";

  /** Token expected at the beginning of a json schema definition. */
  public static final String SCHEMA_TOKEN = "schema:";

  /** Token indicating that this entry should not be imported. */
  public static final String SKIP_ENTRY_TOKEN = "skip-entry";

  /** Delimiter to mark the end of a json schema definition. */
  private static final String EMPTY_LINE = "";

  /** Pairs a qualifier with a writer Schema. */
  public static class QualifierInfo {
    /** The qualifier. */
    private final String mQualifier;

    /** The Schema. */
    private final Schema mSchema;

    /**
     * Wraps a qualifier and schema.  Both must either be null or non-null.
     *
     * @param qualifier The qualifier to wrap.
     * @param schema The Schema to wrap.
     */
    public QualifierInfo(String qualifier, Schema schema) {
      if ((null == qualifier && null != schema)
          || (null != qualifier && null == schema)) {
        throw new IllegalArgumentException("Qualifier and schema must both be null or non-null.");
      }
      mQualifier = qualifier;
      mSchema = schema;
    }

    /**
     * Returns whether the qualifier and schema are both null.
     * If false, then both the qualifier and schema are non-null.
     *
     * @return whether the qualifier and schema are both null.
     */
    public boolean isEmpty() {
      return null == mQualifier;
    }

    /**
     * Returns the wrapped qualifier.
     *
     * @return The wrapped qualifer.
     */
    public String getQualifier() {
      return mQualifier;
    }

    /**
     * Returns the wrapped Schema.
     *
     * @return The wrapped Schema.
     */
    public Schema getSchema() {
      return mSchema;
    }
  }

  /** An unmodifiable List of QualifierInfo loaded from parse() method. */
  private List<QualifierInfo> mQualifierInfo = null;

  /**
   * Returns the unmodifiable List of QualifierInfo loaded from the <code>parse()</code> method,
   * or null if <code>parse()</code> has not been called.
   *
   * @return ColumnInfo, or null if <code>parse()</code> has not been called.
   */
  public List<QualifierInfo> getQualifierInfo() {
    return mQualifierInfo;
  }

  /**
   * Loads internal references to Column names, and Schemas.
   *
   * @param in The InputStream to parse.
   * @throws IOException if input stream cannot be read.
   */
  public void parse(InputStream in) throws IOException {
    InputStreamReader isr = null;
    BufferedReader layoutReader = null;
    try {
      isr = new InputStreamReader(in, "UTF-8");
      layoutReader = new BufferedReader(isr);
      List<String> rawQualifiers = new ArrayList<String>();
      List<String> rawSchemas = new ArrayList<String>();

      String qualifier;
      String json;
      String line = layoutReader.readLine();
      while (line != null) {
        // Skip empty lines.
        if (line.equals(EMPTY_LINE)) {
          line = layoutReader.readLine();
          continue;
        }

        if (SKIP_ENTRY_TOKEN.equals(line)) {
          // null entries indicate this column should be skipped.
          qualifier = null;
          json = null;
        } else {
          qualifier = parseName(line);
          json = parseSchema(layoutReader);
        }

        rawQualifiers.add(qualifier);
        rawSchemas.add(json);

        line = layoutReader.readLine();
      }

      mQualifierInfo = Collections.unmodifiableList(makeQualifierInfo(rawQualifiers, rawSchemas));

      LOG.info("Parsed input layout of size: " + getQualifierInfo().size());
    } finally {
      IOUtils.closeQuietly(layoutReader);
      IOUtils.closeQuietly(isr);
    }
  }

  /**
   * Creates a List of QualifierInfo from a List of raw qualifiers and schemas.
   * Creates an empty QualifierInfo if qualifiers and rawSchemas are null for
   * an entry (Since this is how parse() indicates that a qualifier/Schema pair
   * should be skipped).
   *
   * Throws an AvroRuntimeException if one of the schemas in rawSchemas cannot be parsed.
   * Throws an IllegalArgumentException if only one of a qualifier and its matching
   * raw schema are null.
   *
   * @param qualifiers The set of qualifiers to store.
   * @param rawSchemas The set of Avro Schemas (as json) to parse and store.
   * @return A List of QualifierInfo matching qualifiers to Avro Schemas.
   */
  private static List<QualifierInfo> makeQualifierInfo(List<String> qualifiers,
      List<String> rawSchemas) {
    assert null != qualifiers && null != rawSchemas
        && qualifiers.size() == rawSchemas.size();

    List<QualifierInfo> qualifierInfo = new ArrayList<QualifierInfo>(qualifiers.size());
    for (int i = 0; i < qualifiers.size(); i++) {
      if (null != qualifiers.get(i) && null != rawSchemas.get(i)) {
        Schema schema = new Schema.Parser().parse(rawSchemas.get(i));
        qualifierInfo.add(new QualifierInfo(qualifiers.get(i), schema));
      } else if (null == qualifiers.get(i) && null == qualifiers.get(i)) {
        qualifierInfo.add(new QualifierInfo(null, null));
      } else {
        throw new IllegalArgumentException("Qualifier and schema must both be non-null or "
            + "both be null.  This is not the case at index: " + i);
      }
    }
    assert qualifiers.size() == qualifierInfo.size();

    return qualifierInfo;
  }

  /**
   * Reads one line of layoutReader and returns the name listed there.
   * Expects a format of "qualifier:"[name].
   *
   * @param line The line to parse name from.
   * @return The name listed after "qualifier:" on the current line
   */
  private static String parseName(String line) {
    if (null == line || !line.startsWith(QUALIFIER_TOKEN)) {
      throw new RuntimeException(
          "Expected one line naming a qualifier.  Line found was: " + line);
    }

    String name = line.substring(QUALIFIER_TOKEN.length());
    LOG.debug("Found qualifier: " + name);
    return name;
  }

  /**
   * Reads one or more lines of layoutReader and returns the json string there.
   * Expects a format of "schema:"[json_schema].
   *
   * @param layoutReader The reader.
   * @return The json schema definition at this position in the reader.
   * @throws IOException if file cannot be read.
   */
  private static String parseSchema(BufferedReader layoutReader) throws IOException {
    StringBuilder sb = new StringBuilder();
    String line = layoutReader.readLine();

    // First line must start with the SCHEMA_TOKEN
    if (null == line || !line.startsWith(SCHEMA_TOKEN)) {
      throw new RuntimeException(
          "Expected line beginning with '" + SCHEMA_TOKEN + "'.  Line found was: " + line);
    }
    sb.append(line.substring(SCHEMA_TOKEN.length()));

    // Add lines to json string until an EMPTY_LINE is found
    line = layoutReader.readLine();
    while (line != null && !line.equals(EMPTY_LINE)) {
      // Catch user-error of putting another qualifier token immediately after the
      // schema line. We need the blank line to absorb the end of the schema definition.
      if (line.startsWith(QUALIFIER_TOKEN) || line.trim().equals(SKIP_ENTRY_TOKEN)) {
        throw new IOException("Found an unexpected line: [" + line.trim() + "]. "
            + "Input layout entries must be separated by blank lines.");
      }

      // This next line is just a continuation of the schema. Keep reading and appending.
      sb.append(line);
      line = layoutReader.readLine();
    }

    return sb.toString();
  }
}
