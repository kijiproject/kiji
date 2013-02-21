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

package org.kiji.mapreduce.lib.bulkimport;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.lib.util.CSVParser;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;

/**
 * Bulk importer that handles comma separated files.  TSVs are also supported by setting the
 * <code>kiji.import.text.field.separator</code> configuration item specified by
 * {@link #CONF_FIELD_DELIMITER}.  This bulk importer uses
 * {@link org.kiji.mapreduce.lib.util.CSVParser} for parsing lines into fields.
 *
 * A default header row can be specified by setting the
 * <code>kiji.import.text.column.header_row</code> configuration item specified by
 * {@link #CONF_INPUT_HEADER_ROW}.  If this is not specified, this bulk importer will infer
 * headers from the first line of text encountered.  Not that within a MapReduce job this is not
 * necessarily the first line of the file and thus this parameter should be set.
 *
 * <h2>Creating a bulk import job for CSV:</h2>
 * <p>
 *   The CSV bulk importer can be passed into a
 *   {@link org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder}.  A
 *   {@link KijiTableImportDescriptor}, which defines the mapping from the import fields to the
 *   destination Kiji columns, must be passed in as part of the job configuration.  For writing
 *   to an HFile which can later be loaded with the <code>kiji bulk-load<code> tool the job
 *   creation looks like:
 * </p>
 * <pre><code>
 *   // Set the import descriptor file to be used for this bulk importer.
 *   conf.set(DescribedInputTextBulkImporter.CONF_FILE, "foo-test-import-descriptor.json");
 *
 *   // Set the header line.
 *   conf.set(CSVBulkImporter.CONF_INPUT_HEADER_ROW, "first,last,email,phone");
 *
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = KijiBulkImportJobBuilder.create()
 *       .withConf(conf)
 *       .withBulkImporter(CSVBulkImporter.class)
 *       .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
 *       .withOutput(new HFileMapReduceJobOutput(mOutputTable, hfileDirPath))
 *       .build();
 * </code></pre>
 * <p>
 *   Alternately the bulk importer can be configured to write directly to a Kiji Table.  This is
 *   <em>not recommended</em> because it generates individual puts for each cell that is being
 *   written. For small jobs or tests, a direct Kiji table output job can be created by modifying
 *   out the .withOutput parameter to:
 *   <code>.withOutput(new DirectKijiTableMapReduceJobOutput(mOutputTable))</code>
 * </p>
 *
 * @see KijiTableImportDescriptor
 */
@ApiAudience.Public
public final class CSVBulkImporter extends DescribedInputTextBulkImporter {
  private static final Logger LOG =
      LoggerFactory.getLogger(CSVBulkImporter.class);

  /** Configuration variable for a header row containing delimited string of names of fields. */
  public static final String CONF_INPUT_HEADER_ROW = "kiji.import.text.column.header_row";

  /** Configuration variable that specifies the cell value separator in the text input files. */
  public static final String CONF_FIELD_DELIMITER = "kiji.import.text.field.separator";

  private static final String CSV_DELIMITER = ",";
  private static final String TSV_DELIMITER = "\t";

  /** The string that separates the columns of data in the input file. */
  @HadoopConf(key=CONF_FIELD_DELIMITER)
  private String mColumnDelimiter = CSV_DELIMITER;

  /** Internal map of field names to field positions in the parsed line. */
  private Map<String, Integer> mFieldMap = null;

  /** {@inheritDoc} */
  @Override
  public void setupImporter(KijiTableContext context) throws IOException {
    // Validate that the passed in delimiter is one of the supported options.
    List<String> validDelimiters = Lists.newArrayList(CSV_DELIMITER, TSV_DELIMITER);
    if (!validDelimiters.contains(mColumnDelimiter)) {
      throw new IOException(
          String.format("Invalid delimiter '%s' specified.  Valid options are: '%s'",
          mColumnDelimiter, StringUtils.join(validDelimiters, "','")));
    }

    // If the header row is specified in the configuration, use that.
    if (getConf().get(CONF_INPUT_HEADER_ROW) != null) {
      List<String> fields = null;
      String confInputHeaderRow = getConf().get(CONF_INPUT_HEADER_ROW);
      try {
        fields = split(confInputHeaderRow);
      } catch (ParseException pe) {
        LOG.error("Unable to parse header row: {} with exception {}",
            confInputHeaderRow, pe.getMessage());
        throw new IOException("Unable to parse header row: " + confInputHeaderRow);
      }
      initializeHeader(fields);
    }
  }

  /**
   * Initializes the field to column position mapping for this file.
   * @param headerFields the header fields for this delimited file.
   */
  private void initializeHeader(List<String> headerFields) {
    LOG.info("Initializing field map with fields: " + StringUtils.join(headerFields, ","));
    Map<String, Integer> fieldMap = Maps.newHashMap();
    for (int index=0; index < headerFields.size(); index++) {
      fieldMap.put(headerFields.get(index), index);
    }
    mFieldMap = ImmutableMap.copyOf(fieldMap);
  }

  /**
   * Wrapper around CSV or TSV parsers based on the configuration of this job builder.
   * @return a list of fields split by the mColumnDelimiter.
   * @param line the line to split
   * @throws ParseException if the parser encounters an error while parsing
   */
  private List<String> split(String line) throws ParseException {
    if (CSV_DELIMITER.equals(mColumnDelimiter)) {
      return CSVParser.parseCSV(line);
    } else if (TSV_DELIMITER.equals(mColumnDelimiter)) {
      return CSVParser.parseTSV(line);
    }
    throw new ParseException("Unrecognized delimiter: " + mColumnDelimiter, 0);
  }

  /**
   * Generates the entity id for this imported line using the source from the import descriptor.
   * Called within the produce() method.
   *
   * @param fields One line of input text split on the column delimiter.
   * @param context The context used by the produce() method.
   * @return The EntityId for the data that gets imported by this line.
   */
  protected EntityId getEntityId(List<String> fields, KijiTableContext context) {
    //TODO(KIJIMRLIB-3) Extend this to support composite row key ids
    String rowkey = fields.get(mFieldMap.get(getEntityIdSource()));
    return context.getEntityId(rowkey);
  }

  /**
   * Generates the timestamp for this imported line using the source from the import descriptor.
   * Called within the produce() method.
   *
   * @param fields One line of input text split on the column delimiter.
   * @return The timestamp to be used for this row of data.
   */
  protected Long getTimestamp(List<String> fields) {
    String timestampString = fields.get(mFieldMap.get(getTimestampSource()));
    Long timestamp = Long.parseLong(timestampString);
    return timestamp;
  }

  /** {@inheritDoc} */
  @Override
  public void produce(Text value, KijiTableContext context) throws IOException {
    // This is the header line since fieldList isn't populated
    if (mFieldMap == null) {
      List<String> fields = null;
      try {
        fields = split(value.toString());
      } catch (ParseException pe) {
        LOG.error("Unable to parse header row: {} with exception {}",
            value.toString(), pe.getMessage());
        throw new IOException("Unable to parse header row: " + value.toString());
      }
      initializeHeader(fields);
      // Don't actually import this line
      return;
    }

    List<String> fields = null;
    try {
      fields = split(value.toString());
    } catch (ParseException pe) {
      reject(value, context, pe.toString());
      return;
    }

    List<String> emptyFields = Lists.newArrayList();
    for (KijiColumnName kijiColumnName : getDestinationColumns()) {
      final EntityId eid = getEntityId(fields, context);
      String source = getSource(kijiColumnName);

      if (mFieldMap.get(source) < fields.size()) {
        String fieldValue = fields.get(mFieldMap.get(source));
        if (!fieldValue.isEmpty()) {
          String family = kijiColumnName.getFamily();
          String qualifier = kijiColumnName.getQualifier();
          if (isOverrideTimestamp()) {
            // Override the timestamp from the imported source
            Long timestamp = getTimestamp(fields);
            context.put(eid, family, qualifier, timestamp, convert(kijiColumnName, fieldValue));
          } else {
            // Use the system time as the timestamp
            context.put(eid, family, qualifier, convert(kijiColumnName, fieldValue));
          }
        } else {
          emptyFields.add(source);
        }
      }
    }
    if (!emptyFields.isEmpty()) {
      incomplete(value, context, "Record is missing fields: " + StringUtils.join(emptyFields, ","));
    }

  }
}
