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

package org.kiji.mapreduce.lib.bulkimport;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.hadoop.configurator.HadoopConfigurator;
import org.kiji.mapreduce.KijiBulkImporter;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiTableImportDescriptor;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * DescribedInputTextBulkImporter is an abstract class that provides methods to bulk importers for
 * mapping from source fields in the import lines to destination Kiji columns.
 *
 * Importing from a text file requires specifying a KijiColumnName, and the source field
 * for each element to be inserted into kiji, in addition to the raw import data.  This information
 * is provided by {@link org.kiji.mapreduce.bulkimport.KijiTableImportDescriptor} which is set via
 * the <code>kiji.import.text.input.descriptor.path</code> parameter in {@link #CONF_FILE}.
 *
 * <p>Use this Mapper over text files to import data into a Kiji
 * table.  Each line in the file will be treated data for one row.
 * This line should generate a single EntityId to write to, and any number
 * of writes to add to that entity.  Override the produce(String, Context)
 * method with this behavior.</p>
 *
 * Extensions of this class should implement the following methods:
 * <ul>
 *   <li>{@link #produce} - actual producer code for the bulk importer should go here</li>
 *   <li>{@link #setupImporter} - (optional) any specific setup for this bulk importer.</li>
 *   <li>{@link #cleanupImporter} - (optional) any specific cleanup for this bulk importer.</li>
 * </ul>
 *
 * Extensions of this class can use the following methods to implement their producers:
 * <ul>
 *   <li>{@link #getDestinationColumns} - to retrieve a collection of destination columns.</li>
 *   <li>{@link #getSource} - to retrieve the source for one of the columns listed above.</li>
 *   <li>{@link #getEntityIdSource()} - to retrieve the source for the entity id for the row.</li>
 * </ul>
 */

@ApiAudience.Public
@Inheritance.Extensible
public abstract class DescribedInputTextBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(DescribedInputTextBulkImporter.class);

  /**
   * Location of writer layout file.  File names columns and schemas, and implies
   * ordering of columns in delimited read-in file.
   */
  public static final String CONF_FILE = "kiji.import.text.input.descriptor.path";

  /** Table layout of the output table. */
  private KijiTableLayout mOutputTableLayout;

  /** Table import descriptor for this bulk load. */
  private KijiTableImportDescriptor mTableImportDescriptor;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);

    KijiURI uri;
    try {
      uri = KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build();
    } catch (KijiURIException kue) {
      throw new RuntimeException(kue);
    }

    Kiji kiji = null;
    try {
      kiji = Kiji.Factory.open(uri, conf);

      KijiTable table = null;
      try {
        table = kiji.openTable(uri.getTable());
        mOutputTableLayout = table.getLayout();
      } finally {
        IOUtils.closeQuietly(table);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /**
   * Performs validation that this table import descriptor can be applied to the output table.  This
   * method is final to prevent it from being overridden without being called.  Subclasses should
   * override the setupImporter() method instead of overriding this method.
   * {@inheritDoc}
   */
  @Override
  public final void setup(KijiTableContext context) throws IOException {
    Preconditions.checkNotNull(mTableImportDescriptor);
    Preconditions.checkNotNull(mOutputTableLayout);
    mTableImportDescriptor.validateDestination(mOutputTableLayout);

    setupImporter(context);
  }

  /**
   * Extensible version of {@link org.kiji.mapreduce.KijiBulkImporter#setup} for subclasses of
   * DescribedInputTextBulkImporter.
   * Does nothing by default.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   * @throws IOException on I/O error.
   */
  public void setupImporter(KijiTableContext context) throws IOException {}

  /**
   * Converts a line of text to a set of writes to <code>context</code>, and
   * an EntityId for the row.
   *
   * @param line The line to parse.
   * @param context The context to write to.
   * @throws IOException if there is an error.
   */
  public abstract void produce(Text line, KijiTableContext context)
      throws IOException;

  /**
   * Subclasses should implement the produce(Text line, KijiTableContext context) method instead.
   * {@inheritDoc}
   */
  @Override
  public final void produce(LongWritable fileOffset, Text line, KijiTableContext context)
      throws IOException {
    produce(line, context);
  }

  /** @return an unmodifiable collection of the columns for this bulk importer. */
  protected final Collection<KijiColumnName> getDestinationColumns() {
    Set<KijiColumnName> columns = mTableImportDescriptor.getColumnNameSourceMap().keySet();
    return Collections.unmodifiableSet(columns);
  }

  /**
   * Returns the source for the specified column, or null if the specified column is not a
   * destination column for this importer.
   *
   * @param kijiColumnName the requested Kiji column
   * @return the source for the requested column
   */
  protected final String getSource(KijiColumnName kijiColumnName) {
    return mTableImportDescriptor.getColumnNameSourceMap().get(kijiColumnName);
  }

  /** @return the source for the EntityId. */
  protected final String getEntityIdSource() {
    return mTableImportDescriptor.getEntityIdSource();
  }

  /**
   * Subclasses should implement the {@link #cleanupImporter(KijiTableContext)} method instead.
   * {@inheritDoc}
   */
  @Override
  public final void cleanup(KijiTableContext context) throws IOException {
    cleanupImporter(context);
  }

  /**
   * Extensible version of {@link org.kiji.mapreduce.KijiBulkImporter#cleanup} for subclasses of
   * DescribedInputTextBulkImporter.
   * Does nothing by default.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   * @throws IOException on I/O error.
   */
  public void cleanupImporter(KijiTableContext context) throws IOException {}


  /**
   * Sets the input descriptor.
   *
   * @param inputDescriptorFile The input descriptor path.
   */
  @HadoopConf(key=CONF_FILE, usage="The input descriptor file.")
  protected void setInputDescriptorPath(String inputDescriptorFile) {

    if (null == inputDescriptorFile || inputDescriptorFile.isEmpty()) {
      // Remind the user to specify this path.
      LOG.error("No input-descriptor path specified.");
      throw new RuntimeException("No input descriptor file specified on the Configuration."
          + "  Did you specify the " + CONF_FILE + " variable?");
    }

    Path descriptorPath = new Path(inputDescriptorFile);
    try {
      LOG.info("Parsing input-descriptor file: " + descriptorPath.toString());
      FileSystem fs = descriptorPath.getFileSystem(getConf());
      FSDataInputStream inputStream = fs.open(descriptorPath);
      mTableImportDescriptor =
          KijiTableImportDescriptor.createFromEffectiveJson(inputStream);

    } catch (IOException ioe) {
      LOG.error("Could not read input-descriptor file: " + descriptorPath.toString());
      throw new RuntimeException("Could not read file: " + descriptorPath.toString());
    }
  }
}

