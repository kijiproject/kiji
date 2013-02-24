package com.wibidata.lang;

import java.io.IOException;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowData;

import com.google.common.collect.Lists;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("rawtypes")
public class KijiScheme
    extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  private static final long serialVersionUID = 1L;

  private final KijiDataRequest mRequest;

  public KijiScheme(KijiDataRequest request) {
    mRequest = request;

    final List<Fields> columnFields = Lists.newArrayList();
    for (KijiDataRequest.Column column : request.getColumns()) {
      // TODO: Support data requests with column families.
      final String fieldName = column.getFamily() + "_" + column.getQualifier();

      columnFields.add(new Fields(fieldName));
    }
    final Fields[] fields = columnFields.toArray(new Fields[0]);

    setSourceFields(Fields.join(fields));
  }

  public KijiDataRequest getDataRequest() {
    return mRequest;
  }

  /** {@inheritDoc} */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    final Object[] pair = new Object[] {
      sourceCall.getInput().createKey(),
      sourceCall.getInput().createValue()
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

    // Store the retrieved columns in the tuple.
    // TODO: Ensure that map-type families get populated with the same tuple ordering of columns each time.
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
    return true;
  }

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
    // TODO: Implement.
  }

  /** {@inheritDoc} */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    // TODO: Implement
  }


  // TODO: Implement equals, hashCode.
}
