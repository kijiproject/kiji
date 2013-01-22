package org.kiji.testing.fakehtable

import java.io.DataInput
import java.io.DataOutput
import java.util.{List => JList}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase

/** Pass-through HBase filter, ie. behaves as if there were no filter. */
object PassThroughFilter extends FilterBase {
  override def readFields(in: DataInput): Unit = {
    // nothing to read
  }

  override def write(out: DataOutput): Unit = {
    // nothing to write
  }
}
