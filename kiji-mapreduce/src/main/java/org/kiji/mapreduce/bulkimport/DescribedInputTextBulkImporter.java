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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.hadoop.configurator.HadoopConfigurator;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.bulkimport.TextInputDescriptorParser.QualifierInfo;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;

/**
 * Importing from a text file requires specifying a KijiColumnName, and writer Schema
 * for each element to be inserted into kiji, in addition to the raw import data.
 *
 * <p>This class reads a list of qualifiers and writer Schemas from an input-descriptor file
 * in hdfs.  See {@link org.kiji.mapreduce.bulkimport.TextInputDescriptorParser} for
 * input-descriptor format.</p>
 *
 * <p>The KijiColumnNames, and paired Schemas may be retrieved with the method
 * getColumnInfo().</p>
 *
 * @see org.kiji.mapreduce.bulkimport.TextInputDescriptorParser
 */
public abstract class DescribedInputTextBulkImporter extends BaseTextBulkImporter {
  private static final Logger LOG = LoggerFactory.getLogger(DescribedInputTextBulkImporter.class);

  /** Configuration variable specifying the kiji column family to write to. */
  public static final String CONF_OUTPUT_LOCALITY_GROUP = "kiji.import.text.column.locality_group";

  @HadoopConf(key=CONF_OUTPUT_LOCALITY_GROUP, usage="Kiji locality group to write to.")
  private String mOutputLocalityGroup;

  private String mLocalityGroup;

  /**
   * Location of writer layout file.  File names columns and schemas, and implies
   * ordering of columns in delimited read-in file.
   */
  public static final String CONF_FILE = "kiji.import.text.input.descriptor.path";

  /** Wraps a KijiColumnName and Avro Schema. */
  public static class ColumnInfo {
    /** The KijiColumnName. */
    private final KijiColumnName mKijiColumnName;
    /** The Schema. */
    private final Schema mSchema;

    /**
     * Wraps a KijiColumnName and Schema.
     * Either both must be non-null, or both must be null.
     * Throws an IllegalArgumentException if this is not the case.
     *
     * @param kijiColumnName The KijiColumnName to wrap.
     * @param schema The Avro Schema to wrap.
     */
    public ColumnInfo(KijiColumnName kijiColumnName, Schema schema) {
      if ((null == kijiColumnName && null != schema)
          || (null != kijiColumnName && null == schema)) {
        throw new IllegalArgumentException("kijiColumnName and schema must either both be "
            + "non-null, or else both be null.");
      }
      mKijiColumnName = kijiColumnName;
      mSchema = schema;
    }

    /**
     * Returns whether this ColumnInfo is empty.  This indicates that both
     * <code>getKijiColumnName()</code> and <code>getSchema()</code> will return null.
     * If false, then both these methods will return non-null values.
     *
     * @return Whether this ColumnInfo is empty.
     */
    public boolean isEmpty() {
      return null == mKijiColumnName;
    }

    /** @return The wrapped KijiColumnName. */
    public KijiColumnName getKijiColumnName() {
      return mKijiColumnName;
    }

    /** @return The wrapped Schema. */
    public Schema getSchema() {
      return mSchema;
    }
  }

  /** List of ColumnInfo, pairing KijiColumnNames to Avro writer Schemas. Created by setConf(). */
  private List<ColumnInfo> mColumnInfo;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);

    KijiURI uri;
    try {
      uri = KijiURI.parse(conf.get(KijiConfKeys.OUTPUT_KIJI_TABLE_URI));
    } catch (KijiURIException kue) {
      throw new RuntimeException(kue);
    }

    Kiji kiji = null;
    try {
      kiji = Kiji.open(uri, conf);

      KijiTable table = null;
      try {
        table = kiji.openTable(uri.getTable());

        mLocalityGroup = table.getLayout().getFamilyMap()
            .get(mOutputLocalityGroup).getLocalityGroup().getName();
      } finally {
        IOUtils.closeQuietly(table);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      IOUtils.closeQuietly(kiji);
    }
  }

  /**
   * Returns the family to write to, specified on the Configuration variable
   * "kiji.import.text.output.family".
   * Throws a RuntimeException if this variable has not been specified.
   *
   * @return The family to write to.
   */
  protected String getFamily() {
    if ((null == mOutputLocalityGroup) || mOutputLocalityGroup.isEmpty()) {
      throw new RuntimeException(String.format(
          "Must specify the output column family in the configuration. "
          + "Try using the -D%s option.", CONF_OUTPUT_LOCALITY_GROUP));
    }
    return mOutputLocalityGroup;
  }

  /** {@inheritDoc} */
  @Override
  public String getLocalityGroup() {
    return mLocalityGroup;
  }

  /**
   * Returns a List of ColumnInfo, from a list of QualifierInfo and a family.
   *
   * @param family The family of each KijiColumnName in the generated ColumnInfos.
   * @param qualifierInfo The list of QualifierInfo.
   * @return a List of ColumnInfo.
   */
  private static List<ColumnInfo> createColumnInfo(String family,
      List<QualifierInfo> qualifierInfo) {
    assert null != family && null != qualifierInfo;
    List<ColumnInfo> columnInfo = new ArrayList<ColumnInfo>(qualifierInfo.size());
    for (QualifierInfo info : qualifierInfo) {
      if (info.isEmpty()) {
        columnInfo.add(new ColumnInfo(null, null));
      } else {
        columnInfo.add(new ColumnInfo(
            new KijiColumnName(family, info.getQualifier()), info.getSchema()));
      }
    }
    return columnInfo;
  }

  /**
   * Returns the unmodifiable List of ColumnInfo, or null if setConf() has not been called.
   *
   * @return The List of ColumnInfo, or null if setConf() has not been called.
   */
  protected List<ColumnInfo> getColumnInfo() {
    return mColumnInfo;
  }

  /**
   * Returns the ColumnInfo at index <code>idx</code>.
   * Throws a NullPointerException if setConf() has not been called.
   *
   * @param idx The index of the ColumnInfo to return.
   * @return The ColumnInfo at <code>idx</code>.
   */
  protected ColumnInfo getColumnInfo(int idx) {
    return mColumnInfo.get(idx);
  }

  /**
   * Sets the input descriptor path.
   *
   * @param inputDescriptorFile The input descriptor path.
   */
  @HadoopConf(key=CONF_FILE, usage="The input descriptor file.")
  protected void setInputDescriptorPath(String inputDescriptorFile) {
    if (null == inputDescriptorFile || inputDescriptorFile.isEmpty()) {
      // Remind the user to specify this path.
      LOG.error("No input-descpriptor path specified.");
      throw new RuntimeException("No input descriptor file specified on the Configuration."
          + "  Did you specify the " + CONF_FILE + " variable?");
    }

    Path path = new Path(inputDescriptorFile);
    try {
      TextInputDescriptorParser layout = getWriterLayout(path);
      mColumnInfo = Collections.unmodifiableList(
          createColumnInfo(getFamily(), layout.getQualifierInfo()));
    } catch (IOException ioe) {
      LOG.error("Could not read input-descriptor file: " + path.toString());
      throw new RuntimeException("Could not read file: " + path.toString());
    }
  }

  /**
   * Finds the input layout file in hdfs, and parses to a TextInputDescriptorParser.
   *
   * @param descriptorPath The path to read the descriptor file from.
   * @return The parsed table layout.
   * @throws IOException if file cannot be read from hdfs.
   */
  private TextInputDescriptorParser getWriterLayout(Path descriptorPath) throws IOException {
    LOG.info("Parsing input-descriptor file: " + descriptorPath.toString());
    FileSystem fs = descriptorPath.getFileSystem(getConf());
    FSDataInputStream inputStream = fs.open(descriptorPath);

    TextInputDescriptorParser layoutParser = new TextInputDescriptorParser();
    layoutParser.parse(inputStream);
    return layoutParser;
  }
}

