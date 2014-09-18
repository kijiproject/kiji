import org.apache.hadoop.mapred.InputSplit;

/**
 * Split for a Kiji table that has been extended to support hadoop's older 'mapred' api.
 */
public class KijiTableSplit
    extends org.kiji.mapreduce.KijiTableSplit
    implements InputSplit {

  /** The default constructor. */
  public KijiTableSplit() {
    super();
  }

  /**
   * Create a new KijiTableSplit instance from an HBase TableSplit.
   * @param tableSplit the HBase TableSplit to clone.
   * @param regionStartKey the starting key of the region associated with this split.
   */
  public KijiTableSplit(TableSplit tableSplit, byte[] regionStartKey) {
    super(tableSplit, regionStartKey);
  }
}
