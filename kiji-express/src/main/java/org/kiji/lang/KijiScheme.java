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

package org.kiji.lang;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.base.Objects;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;

/**
 * A scheme that can source and sink data from a Kiji table. This scheme is responsible for
 * converting rows from a Kiji table that are input to a Cascading flow into Cascading tuples (see
 * {@link #source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)}) and writing output
 * data from a Cascading flow to a Kiji table
 * (see {@link #sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)}).
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiScheme is serialized,
 * the result is not persisted anywhere making serialVersionUID unnecessary.
 */
@SuppressWarnings({ "rawtypes", "serial" })
@ApiAudience.Framework
@ApiStability.Unstable
public final class KijiScheme
    extends Scheme<JobConf, RecordReader, OutputCollector, KijiValue, KijiTableWriter> {
  /** Field name containing a row's {@link EntityId}. */
  public static final String ENTITYID_FIELD = "entityid";
  /** Seperator used when using symbols to address columns. */
  public static final String COLUMN_SEPERATOR = "_";

  /** A data request used when this scheme reads from a Kiji table. */
  private final KijiDataRequest mRequest;
  /** A mapping from Cascading tuple field names to Kiji column names. */
  private final Map<String, Column> mColumns;

  /**
   * Creates a new scheme for input and/or output to a Kiji table.
   *
   * @param columns mapping tuple field names to Kiji column names.
   */
  public KijiScheme(Map<String, Column> columns) {
    final Fields[] fields = new Fields[columns.size() + 1];
    fields[0] = new Fields(ENTITYID_FIELD);
    final KijiDataRequestBuilder requestBuilder = KijiDataRequest.builder();

    int i = 1;
    for (Map.Entry<String, Column> entry : columns.entrySet()) {
      final Fields field = new Fields(entry.getKey());
      final KijiColumnName columnName = new KijiColumnName(entry.getValue().name());
      final Column.InputOptions inputOptions = entry.getValue().inputOptions();

      requestBuilder.newColumnsDef()
          .withMaxVersions(inputOptions.maxVersions())
          .withFilter(inputOptions.filter())
          .add(columnName);

      fields[i] = field;
      i++;
    }
    mRequest = requestBuilder.build();
    mColumns = columns;

    setSourceFields(Fields.join(fields));
    setSinkFields(Fields.join(fields));
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    // Write all the required values to the job's configuration object.
    conf.setInputFormat(KijiInputFormat.class);
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(mRequest));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, serializedRequest);
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> process,
      SourceCall<KijiValue, RecordReader> sourceCall) {
    sourceCall.setContext((KijiValue) sourceCall.getInput().createValue());
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   * @throws IOException If there is an error while reading the row from Kiji.
   * @return True always. This is used to indicate if there are more rows to read.
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean source(FlowProcess<JobConf> process,
      SourceCall<KijiValue, RecordReader> sourceCall) throws IOException {
    // Get the current key/value pair.
    final KijiValue value = sourceCall.getContext();
    if (!sourceCall.getInput().next(null, value)) {
      return false;
    }
    final KijiRowData row = value.get();

    final Tuple result = rowToTuple(mColumns, getSourceFields(), row);
    sourceCall.getIncomingEntry().setTuple(result);
    return true;
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  @Override
  public void sourceCleanup(FlowProcess<JobConf> process,
      SourceCall<KijiValue, RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   * @throws IOException If there is an error opening connections to Kiji.
   */
  @Override
  public void sinkPrepare(FlowProcess<JobConf> process,
      SinkCall<KijiTableWriter, OutputCollector> sinkCall) throws IOException {
    // Open a table writer.
    final String uriString = process.getConfigCopy().get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI);
    final KijiURI uri = KijiURI.newBuilder(uriString).build();
    final Kiji kiji = Kiji.Factory.open(uri);
    final KijiTable table = kiji.openTable(uri.getTable());
    final KijiTableWriter writer = table.openTableWriter();
    table.release();
    kiji.release();

    // Store the writer in this scheme's context.
    sinkCall.setContext(writer);
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table. This method is called once
   * for each row on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   * @throws IOException If there is an error while reading the row from Kiji.
   */
  @Override
  public void sink(FlowProcess<JobConf> process,
      SinkCall<KijiTableWriter, OutputCollector> sinkCall) throws IOException {
    // Retrieve writer from the scheme's context.
    final KijiTableWriter writer = sinkCall.getContext();

    // Write the tuple out.
    final TupleEntry output = sinkCall.getOutgoingEntry();
    putTuple(mColumns, getSinkFields(), output, writer);
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   * @throws IOException If there is an error closing connections to Kiji.
   */
  @Override
  public void sinkCleanup(FlowProcess<JobConf> process,
      SinkCall<KijiTableWriter, OutputCollector> sinkCall) throws IOException {
    sinkCall.getContext().close();
    sinkCall.setContext(null);
  }

  /**
   * Converts a KijiRowData to a Cascading tuple.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of desired tuple elements.
   * @param row The row data.
   * @throws IOException if there is an error.
   * @return A tuple containing the values contained in the specified row.
   */
  public static Tuple rowToTuple(Map<String, Column> columns, Fields fields, KijiRowData row)
      throws IOException {
    final Tuple result = new Tuple();
    final Iterator<?> iterator = fields.iterator();

    result.add(row.getEntityId());
    iterator.next();

    // Store the retrieved columns in the tuple.
    while (iterator.hasNext()) {
      final Column column = columns.get(iterator.next().toString());
      final KijiColumnName columnName = new KijiColumnName(column.name());

      result.add(row.getValues(columnName.getFamily(), columnName.getQualifier()));
    }

    return result;
  }

  // TODO(CHOP-35): Use an output format that writes to HFiles.
  /**
   * Writes a Cascading tuple to a Kiji table.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of incoming tuple elements.
   * @param output Tuple to write out.
   * @param writer KijiTableWriter to use to write.
   * @throws IOException if there is an error.
   */
  public static void putTuple(Map<String, Column> columns, Fields fields, TupleEntry output,
      KijiTableWriter writer) throws IOException {
    final EntityId entityId = (EntityId) output.getObject(ENTITYID_FIELD);
    final Iterator<?> iterator = fields.iterator();
    iterator.next();

    // Store the retrieved columns in the tuple.
    while (iterator.hasNext()) {
      final String field = iterator.next().toString();
      final KijiColumnName columnName = new KijiColumnName(columns.get(field).name());

      writer.put(
          entityId,
          columnName.getFamily(),
          columnName.getQualifier(),
          output.getObject(field));
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiScheme)) {
      return false;
    }

    final KijiScheme scheme = (KijiScheme) other;
    return Objects.equal(mRequest, scheme.mRequest)
        && Objects.equal(mColumns, scheme.mColumns);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mRequest, mColumns);
  }
}
