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

package com.wibidata.lang;

import java.io.IOException;
import java.util.List;
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
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
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
 */
@SuppressWarnings("rawtypes")
public class KijiScheme
    extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  /** Schemes must be serialized as part of a Cascading job. */
  private static final long serialVersionUID = 1L;
  /** A data request used when this scheme reads from a Kiji table. */
  private final KijiDataRequest mRequest;
  /**
   * A mapping from Cascading tuple field names to Kiji column names,
   * used when outputting to a Kiji table.
   */
  private final Map<String, String> mOutputSpec;

  /**
   * Creates a new scheme for input and/or output to a Kiji table.
   *
   * @param request that specifies columns to be read from a Kiji table.
   * @param outputSpec mapping tuple field names to Kiji column names.
   */
  public KijiScheme(KijiDataRequest request, Map<String, String> outputSpec) {
    mRequest = request;
    mOutputSpec = outputSpec;

    final List<Fields> columnFields = Lists.newArrayList();
    columnFields.add(new Fields("entityid")); // Add entity ID as first field.
    for (KijiDataRequest.Column column : request.getColumns()) {
      // TODO: Support data requests with column families.
      final String fieldName = column.getFamily() + "_" + column.getQualifier();

      columnFields.add(new Fields(fieldName));
    }
    final Fields[] fields = columnFields.toArray(new Fields[0]);

    setSourceFields(Fields.join(fields));
  }

  /**
   * @return the data request used by this scheme.
   */
  public KijiDataRequest getDataRequest() {
    return mRequest;
  }

  /** {@inheritDoc} */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    final Object[] pair = new Object[] {
      sourceCall.getInput().createKey(),
      sourceCall.getInput().createValue(),
    };
    sourceCall.setContext(pair);
  }

  /** {@inheritDoc} */
  @Override
  public void sourceCleanup(FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    final Tuple result = new Tuple();

    // Get the current key/value pair.
    final KijiKey key = (KijiKey) sourceCall.getContext()[0];
    final KijiValue value = (KijiValue) sourceCall.getContext()[1];
    if (!sourceCall.getInput().next(key, value)) {
      return false;
    }
    final KijiRowData row = value.get();

    result.add(row.getEntityId().toString());

    // Store the retrieved columns in the tuple.
    // TODO: Ensure that map-type families get populated with the same tuple ordering of columns
    // each time.
    for (Column column : mRequest.getColumns()) {
      final String family = column.getFamily();

      if (null == column.getQualifier()) {
        for (String qualifier : row.getQualifiers(family)) {
          result.add(row.getValues(family, qualifier));
        }
      } else {
        final String qualifier = column.getQualifier();
        result.add(row.getValues(family, qualifier));
      }
    }

    sourceCall.getIncomingEntry().setTuple(result);
    return true; }

  /** {@inheritDoc} */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    // Write all the required values to the job's configuration object.
    conf.setInputFormat(KijiInputFormat.class);
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(mRequest));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, serializedRequest);
  }

  /** {@inheritDoc} */
  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
      throws IOException {
    // TODO: Currently this method does the basic thing of using KijiTableWriter.put()
    // TODO (cont) Eventually we want to be able to write these directly to the files that Kiji
    // uses (?)

    // Get a handle to the kiji table and kiji table writer.
    KijiTable kijiTable = (KijiTable) sinkCall.getContext()[0];
    KijiTableWriter kijiTableWriter = (KijiTableWriter) sinkCall.getContext()[1];

    final TupleEntry outgoingEntry = sinkCall.getOutgoingEntry();
    // final OutputCollector outputCollector = sinkCall.getOutput(); // currently unused.

    String id = (String) outgoingEntry.getObject("entityid");

    // For every outputtuple -> column in the specification:
    for (Map.Entry<String, String> entry : mOutputSpec.entrySet()) {
      String colName = (String) entry.getKey();
      String value = (String) outgoingEntry.getObject(entry.getValue());

      Fields fields = getSinkFields();
      EntityId entityId = kijiTable.getEntityId(id);
      String family = colName.split("_")[0];
      String qualifier = colName.split("_")[1];

      kijiTableWriter.put(entityId, family, qualifier, value);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    // TODO: implement
  }

  /** {@inheritDoc} */
  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess,
      SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    // Create the KijiTable and KijiTableWriter to use.
    KijiURI uri = KijiURI.newBuilder(
        flowProcess.getConfigCopy().get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI))
        .build();
    KijiTable kijiTable = Kiji.Factory.open(uri).openTable(uri.getTable());
    KijiTableWriter kijiTableWriter = kijiTable.openTableWriter();
    // Put these in the context to avoid recreating them every tuple.
    sinkCall.setContext(new Object[] {kijiTable, kijiTableWriter});
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiScheme)) {
      return false;
    }

    final KijiScheme scheme = (KijiScheme) other;
    return Objects.equal(mRequest, scheme.mRequest)
        && Objects.equal(mOutputSpec, scheme.mOutputSpec);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mRequest, mOutputSpec);
  }
}
