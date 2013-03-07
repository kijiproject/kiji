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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.base.Objects;
import org.apache.hadoop.conf.Configuration;

import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;

/**
 * A scheme that can source and sink data from a Kiji table.
 *
 * <p>This scheme is responsible for converting rows from a Kiji table that are input to a
 * Cascading flow into Cascading tuples (see
 * {@link #source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)}) and writing output
 * data from a Cascading flow to a Kiji table
 * (see {@link #sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)}). This scheme is meant
 * to be used with {@link LocalKijiTap} and Cascading's local job runner. Jobs run with Cascading's
 * local job runner execute on your local machine instead of a cluster. This can be helpful for
 * testing or quick jobs.</p>
 *
 * <p>Note: Warnings about a missing serialVersionUID are ignored here. When KijiScheme is
 * serialized, the result is not persisted anywhere making serialVersionUID unnecessary.</p>
 */
@SuppressWarnings("serial")
public class LocalKijiScheme
    extends Scheme<Properties, InputStream, OutputStream, Iterator<KijiRowData>, KijiTableWriter> {
  /** A data request used when this scheme reads from a Kiji table. */
  private final KijiDataRequest mRequest;
  /** A mapping from Cascading tuple field names to Kiji column names. */
  private final Map<String, Column> mColumns;

  /** A table reader with an open connection to the desired Kiji table. */
  private KijiTableReader mReader;
  /** A table scanner with an open connection to the desired Kiji table. */
  private KijiRowScanner mScanner;

  /**
   * Constructs a new Kiji scheme for usage with Cascading/Scalding's local mode.
   *
   * @param columns a mapping from field names to requested columns.
   */
  public LocalKijiScheme(Map<String, Column> columns) {
    final Fields[] fields = new Fields[columns.size() + 1];
    fields[0] = new Fields(KijiScheme.ENTITYID_FIELD);
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
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  @Override
  public void sourceConfInit(FlowProcess<Properties> process,
      Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
    // No-op. Setting options in a java Properties object is not going to help us read from
    // a Kiji table.
  }

  /**
   * Sets up any resources required to read from a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   * @throws IOException If there is an error opening connections to Kiji.
   */
  @Override
  public void sourcePrepare(FlowProcess<Properties> process,
      SourceCall<Iterator<KijiRowData>, InputStream> sourceCall) throws IOException {
    final String uriString = process.getStringProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI);
    final KijiURI uri = KijiURI.newBuilder(uriString).build();
    final Kiji kiji = Kiji.Factory.open(uri, new Configuration());
    try {
      final KijiTable table = kiji.openTable(uri.getTable());
      try {
        mReader = table.openTableReader();
        mScanner = mReader.getScanner(mRequest);
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }

    sourceCall.setContext(mScanner.iterator());
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row in the table.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   * @throws IOException If there is an error while reading the row from Kiji.
   * @return <code>true</code> if another row was read and it was converted to a tuple,
   *     <code>false</code> if there were no more rows to read.
   */
  @Override
  public boolean source(FlowProcess<Properties> process,
      SourceCall<Iterator<KijiRowData>, InputStream> sourceCall) throws IOException {
    if (sourceCall.getContext().hasNext()) {
      // Get the current row.
      final KijiRowData row = sourceCall.getContext().next();

      final Tuple result = KijiScheme.rowToTuple(mColumns, getSourceFields(), row);
      sourceCall.getIncomingEntry().setTuple(result);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Cleans up any resources used to read from a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   * @throws IOException If there is an error while cleaning up resources.
   */
  @Override
  public void sourceCleanup(FlowProcess<Properties> process,
      SourceCall<Iterator<KijiRowData>, InputStream> sourceCall) throws IOException {
    mReader.close();
    mScanner.close();
    sourceCall.setContext(null);
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  @Override
  public void sinkConfInit(FlowProcess<Properties> process,
      Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
    // No-op. Setting options in a java Properties object is not going to help us write to
    // a Kiji table.
  }

  /**
   * Sets up any resources required to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   * @throws IOException If there is an error opening connections to Kiji.
   */
  @Override
  public void sinkPrepare(FlowProcess<Properties> process,
      SinkCall<KijiTableWriter, OutputStream> sinkCall) throws IOException {
    // Open a table writer.
    final String uriString = process.getStringProperty(KijiConfKeys.KIJI_OUTPUT_TABLE_URI);
    final KijiURI uri = KijiURI.newBuilder(uriString).build();
    final Kiji kiji = Kiji.Factory.open(uri, new Configuration());
    try {
      final KijiTable table = kiji.openTable(uri.getTable());
      try {
        sinkCall.setContext(table.openTableWriter());
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   * @throws IOException If there is an error while reading the row from Kiji.
   */
  @Override
  public void sink(FlowProcess<Properties> process,
      SinkCall<KijiTableWriter, OutputStream> sinkCall) throws IOException {
    // Retrieve writer from the scheme's context.
    final KijiTableWriter writer = sinkCall.getContext();

    // Write the tuple out.
    final TupleEntry output = sinkCall.getOutgoingEntry();
    KijiScheme.putTuple(mColumns, getSinkFields(), output, writer);
  }

  /**
   * Cleans up any resources used to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   * @throws IOException If there is an error closing connections to Kiji.
   */
  @Override
  public void sinkCleanup(FlowProcess<Properties> process,
      SinkCall<KijiTableWriter, OutputStream> sinkCall) throws IOException {
    sinkCall.getContext().close();
    sinkCall.setContext(null);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LocalKijiScheme)) {
      return false;
    }

    final LocalKijiScheme scheme = (LocalKijiScheme) other;
    return Objects.equal(mRequest, scheme.mRequest)
        && Objects.equal(mColumns, scheme.mColumns);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mRequest, mColumns);
  }
}
