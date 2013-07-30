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
import java.io.StringReader;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;

/**
 * Bulk importer that handles XML files.  The XML file should contain formatted XML such that each
 * record starts and ends with the same tag and that the record delimiting tag is never nested
 * within a record, CDATA element, or comment.  Each record will be imported to a single row.
 * Comments and CDATA containing record delimiting tags will be read as valid tags and may cause the
 * record reader to return invalid records.  Other comments and CDATA will be parsed normally. This
 * bulk importer uses {@link javax.xml.DocumentBuilder} to parse XML records and XPath to convert
 * records to fields.
 *
 * <p>
 *  The record reader used to extract XML records from the source file has limited ability to
 *  understand XML structures and will fail to properly differentiate records if the specified
 *  record delimiting tag occurs within records. For example, the following code will yield a
 *  malformed record because the record reader will find a <code>&lt;/record&gt;</code> tag before
 *  the full record has been read.
 * <pre><code>&lt;record&gt;
 *   &lt;user&gt;
 *     &lt;record&gt;
 *     &lt;/record&gt;
 *   &lt;/user&gt;
 * &lt;/record&gt;</code></pre></p>
 *
 * <h4>An example of valid XML and an input descriptor</h4>
 * <p>
 * <pre><code>&lt;?xml version="1.0" encoding="UTF-8"?&gt;
 *   &lt;users&gt;
 *     &lt;user id=001&gt;
 *       &lt;firstname&gt;John&lt;/firstname&gt;
 *       &lt;lastname&gt;Doe&lt;/lastname&gt;
 *       &lt;email&gt;johndoe@gmail.com&lt;/email&gt;
 *       &lt;phone&gt;202-555-9876&lt;/phone&gt;
 *     &lt;/user&gt;
 *     &lt;user id=002&gt;
 *       &lt;firstname&gt;Alice&lt;/firstname&gt;
 *       &lt;lastname&gt;Smith&lt;/lastname&gt;
 *       &lt;email&gt;alice.smith@yahoo.com&lt;/email&gt;
 *       &lt;phone&gt;123-456-7890&lt;/phone&gt;
 *     &lt;/user&gt;
 *   &lt;/users&gt;
 *
 * {
 *   name : "users",
 *   families : [ {
 *     name : "info",
 *     columns : [ {
 *       name : "id",
 *       source : "user/@id"
 *     }, {
 *       name : "first_name",
 *       source : "//firstname"
 *     }, {
 *       name : "last_name",
 *       source : "//lastname"
 *     }, {
 *       name : "email",
 *       source : "user/email"
 *     }, {
 *       name : "phone",
 *       source : "user/phone"
 *     } ]
 *   } ],
 *   entityIdSource : "user/email"
 *   version : "import-1.0"
 * }</code></pre>
 * </p>
 * <p>
 * To import data from the XML file above using the import descriptor, the record delimiting tag
 * should be set to <code>user</code>.  The source fields in the import descriptor are XPath
 * expressions representing hierarchical paths from the root of the XML document.  Each path begins
 * with "user" instead of "users" because XPath expressions are evaluated against parsed records
 * only, not the original source file.
 * </p>
 *
 * <h4>Common XPath expressions</h4>
 * <p>
 * <code>user/email</code> or <code>user/email[1]</code> returns the text content of the first
 * <code>email</code> tag inside the user record. <code>user/email[2]</code> returns the text
 * content of the second <code>email</code> tag inside the user record. <code>user/@id</code>
 * returns the value of the <code>id</code> attribute of the user tag. <code>//firstname</code>
 * returns the text content of the first <code>firstname</code> tag at any depth within the record.
 * </p>
 *
 * <h2>Creating a bulk import job for XML files:</h2>
 * <p>
 *  The bulk importer can be passed into a
 *   {@link org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder}.  A
 *   {@link KijiTableImportDescriptor}, which defines the mapping from the import fields to the
 *   destination Kiji columns, must be passed in as part of the job configuration.  For writing
 *   to an HFile which can later be loaded with the <code>kiji bulk-load</code> tool the job
 *   creation looks like:
 * </p>
 * <pre><code>
 *   // Set the import descriptor file to be used for this bulk importer.
 *   conf.set(DescribedInputTextBulkImporter.CONF_FILE, "foo-test-xml-import-descriptor.json");
 *   // Set the record delimiting XML tag.
 *   conf.set(XMLInputFormat.RECORD_TAG_CONF_KEY, "user");
 *   // Set the XML header.  This line is unnecessary in this case, because the header matches the
 *   // default value.
 *   conf.set(XMLInputFormat.XML_HEADER_CONF_KEY, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(XMLBulkImporter.class)
        .withInput(MapReduceJobInputs.newXMLMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(mOutputTable, hfileDirPath))
        .build();
 * </code></pre>
 * <p>
 *   Alternately the bulk importer can be configured to write directly to a Kiji Table.  This is
 *   <em>not recommended</em> because it generates individual puts for each cell that is being
 *   written. For small jobs or tests, a direct Kiji table output job can be created by modifying
 *   the .withOutput parameter to:
 *   <code>.withOutput(MapReduceJobOutputs
 *       .newDirectKijiTableMapReduceJobOutput(mOutputTableURI))</code>
 * </p>
 * <h2> Launching an XML bulk import job from the command line:</h2>
 * <p>
 *  When launching an XML bulk import job from the command line, some configuration keys must be set
 *  to allow the bulk importer to properly interpret your files.  Configuration key
 *  "kiji.input.xml.record.tag" should be set to the XML tag used to delimit records (the tag should
 *  be specified without angle brackets as they will be added by the record reader automatically).
 *  "kiji.input.xml.header" should be set to the XML header line containing version and encoding
 *  information (e.g. <tt><?xml version=\"1.0\" encoding=\"UTF-8\"?></tt> is the default header
 *  value) which will be used to interpret records if necessary. Configuration keys should be
 *  specified at the beginning of the argument list to <code>kiji bulk-import</code> because the
 *  keys are needed by other arguments for configuration.
 * </p>
 * <pre><code>
 *  kiji bulk-import \
 *      -Dkiji.input.xml.record.tag=user \
 *      -Dkiji.import.text.input.descriptor.path=foo-test-xml-import-descriptor.json \
 *      --importer=org.kiji.mapreduce.lib.bulkimport.XMLBulkImporter \
 *      --lib=your/lib/dir
 *      --input="format=xml file=TestXMLImportInput.txt" \
 *      --output="format=kiji table=kiji://.env/default/table nsplits=1"
 * </code></pre>
 */
@ApiAudience.Public
public final class XMLBulkImporter extends DescribedInputTextBulkImporter {
  private static final Logger LOG = LoggerFactory.getLogger(XMLBulkImporter.class);

  /** {@inheritDoc} */
  @Override
  public void setupImporter(KijiTableContext context) {
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xPath = xPathFactory.newXPath();
    ArrayList<String> invalidXPaths = Lists.newArrayList();
    try {
      xPath.compile(getEntityIdSource());
    } catch (XPathExpressionException xpee) {
      invalidXPaths.add(getEntityIdSource());
    }
    if (isOverrideTimestamp()) {
      try {
        xPath.compile(getTimestampSource());
      } catch (XPathExpressionException xpee) {
        invalidXPaths.add(getTimestampSource());
      }
    }
    for (KijiColumnName kijiColumnName : getDestinationColumns()) {
      String source = getSource(kijiColumnName);
      try {
        xPath.compile(source);
      } catch (XPathExpressionException xpee) {
        invalidXPaths.add(source);
      }
    }
    if (invalidXPaths.size() != 0) {
      for (String xpath : invalidXPaths) {
        LOG.error("Invalid XPath expression: " + xpath);
      }
      throw new RuntimeException(
          "Invalid XPath Expression(s), details can be found in XMLBulkImporter log file.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void produce(Text xmlText, KijiTableContext context) throws IOException {
    String xml = xmlText.toString();
    // Prepare the document builder and XPath.
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder = null;
    Document document = null;
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xPath = xPathFactory.newXPath();
    try {
      // Parse the record as an XML document
      documentBuilder = documentBuilderFactory.newDocumentBuilder();
      InputSource inputSource = new InputSource();
      inputSource.setCharacterStream(new StringReader(xml));
      try {
        document = documentBuilder.parse(inputSource);
      } catch (SAXException saxe) {
        reject(xmlText, context, "Failed to parse XML.");
        return;
        }
    } catch (ParserConfigurationException pce) {
      // Should be unreachable, since the default configuration is used.
      reject(xmlText, context, "Invalid parser configuration.");
      return;
    }

    // Get the entityId.
    String entityIdStr = null;
    try {
      entityIdStr = xPath.compile(getEntityIdSource()).evaluate(document);
      if (entityIdStr == null || entityIdStr.isEmpty()) {
        reject(xmlText, context, "Unable to retrieve entityId from source field.");
        return;
      }
    } catch (XPathExpressionException xpee) {
      // Should be unreachable, errors caught in setupImporter().
      LOG.error("Invalid XPath expression: " + getEntityIdSource());
      throw new RuntimeException("Invalid XPath expression: " + getEntityIdSource());
    }

    final EntityId eid = context.getEntityId(entityIdStr);

    // Get the timestamp.
    Long timestamp = null;
    if (isOverrideTimestamp()) {
      String timestampSource = null;
      try {
        timestampSource = xPath.compile(getTimestampSource()).evaluate(document);
      } catch (XPathExpressionException xpee) {
        // Should be unreachable, errors caught in setupImporter().
        LOG.error("Invalid XPath expression: " + getTimestampSource());
        throw new RuntimeException("Invalid XPath expression: " + getTimestampSource());
      }
      try {
        final int time = 0;
        timestamp = Long.parseLong(timestampSource);
      } catch (NumberFormatException nfe) {
        incomplete(xmlText, context, "Detected missing field: " + getTimestampSource());
      }
    } else {
      // If timestamp is not overridden in the import descriptor, use the current system time for
      // all writes to this row.
      timestamp = System.currentTimeMillis();
    }

    // For each output column, traverse the XML document with XPath and write the data.
    for (KijiColumnName kijiColumnName : getDestinationColumns()) {

      String source = getSource(kijiColumnName);
      String fieldValue = null;
      try {
        fieldValue = xPath.compile(source).evaluate(document);
      } catch (XPathExpressionException xpee) {
        // Should be unreachable, errors caught in setupImporter().
        LOG.error("Invalid XPath expression: " + source);
        throw new RuntimeException("Invalid XPath expression: " + source);
      }
      if (fieldValue != null && !fieldValue.isEmpty()) {
        String family = kijiColumnName.getFamily();
        String qualifier = kijiColumnName.getQualifier();
        context.put(eid, family, qualifier, timestamp, convert(kijiColumnName, fieldValue));
      } else {
        incomplete(xmlText, context, "Detected missing field: " + source);
      }
    }
  }
}
