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

package org.kiji.mapreduce.kvstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.kiji.mapreduce.KeyValueStore;
import org.kiji.mapreduce.KeyValueStoreConfiguration;

/**
 * Utility that parses an XML file that specifies KeyValueStore implementations
 * to bind in an application.
 */
public class XmlKeyValueStoreParser {
  private static final Logger LOG = LoggerFactory.getLogger(
      XmlKeyValueStoreParser.class.getName());

  /**
   * Given an InputStream pointing to an opened resource that specifies a set of KeyValueStores
   * via XML, return the map of names to configured KeyValueStore instances. The caller is
   * responsible for closing the InputStream.
   *
   * <p>If an XML file tries to bind the same name to multiple stores, this will throw an
   * IOException.</p>
   *
   * @param xmlStream the InputStream pointing to the XML resource to load
   * @return a map from names to configured KeyValueStore instances.
   * @throws IOException if there is an error reading from the input stream or parsing the XML.
   */
  public Map<String, KeyValueStore<?, ?>> loadStoresFromXml(InputStream xmlStream)
      throws IOException {

    Map<String, KeyValueStore<?, ?>> outMap = new HashMap<String, KeyValueStore<?, ?>>();
    try {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(xmlStream);
      Element root = doc.getDocumentElement();
      root.normalize();
      if (!root.getNodeName().equals("stores")) {
        throw new IOException("Expected <stores> as root element.");
      }

      for (int i = 0; i < root.getChildNodes().getLength(); i++) {
        Node node = root.getChildNodes().item(i);
        if (node.getNodeType() != Node.ELEMENT_NODE) {
          continue;
        }

        if (node.getNodeName().equals("store")) {
          NamedNodeMap attrs = node.getAttributes();
          Node nameNode = attrs.getNamedItem("name");
          Node classNode = attrs.getNamedItem("class");
          if (null == nameNode) {
            throw new IOException("Expected 'name' attribute in <store>");
          } else if (null == classNode) {
            throw new IOException("Expected 'class' attribute in <store>");
          }

          String storeName = nameNode.getNodeValue();
          String storeClassStr = classNode.getNodeValue();

          if (storeName.isEmpty()) {
            throw new IOException("Expected non-empty store name");
          } else if (storeClassStr.isEmpty()) {
            throw new IOException("Expected non-empty store class");
          }

          if (outMap.containsKey(storeName)) {
            throw new IOException("Store with name \"" + storeName
                + "\" is defined multiple times");
          }

          // If the store class string does not contain any package specification,
          // auto-append org.kiji.mapreduce.kvstore.
          // TODO(KIJI-364): Make this compatible with user-written kvstores that live
          // in the default package. (Maybe try instantiating them first?)
          if (!storeClassStr.contains(".")) {
            storeClassStr = FileKeyValueStore.class.getPackage().getName()
                + "." + storeClassStr;
          }

          try {
            Class<?> userStoreClass = Class.forName(storeClassStr);

            if (!KeyValueStore.class.isAssignableFrom(userStoreClass)) {
              throw new IOException("Class " + userStoreClass.getName()
                  + " does not extend KeyValueStore");
            }

            @SuppressWarnings("rawtypes")
            Class<? extends KeyValueStore> storeClass =
                userStoreClass.asSubclass(KeyValueStore.class);
            LOG.info("Instantiating " + storeClass.getName() + " for store name " + storeName);

            // Create the store instance, and then let it configure itself by
            // parsing the <store> element.
            KeyValueStore<?, ?> store = ReflectionUtils.newInstance(
                storeClass, new Configuration());
            store.configureFromXml(node, storeName);
            outMap.put(storeName, store);
          } catch (ClassNotFoundException cnfe) {
            throw new IOException("No such class: " + storeClassStr, cnfe);
          }
        } else {
          // We only expect <store> blocks in here.
          throw new IOException("Unexpected first-level element: " + node.getNodeName());
        }
      }
    } catch (ParserConfigurationException pce) {
      throw new IOException(pce);
    } catch (SAXException se) {
      throw new IOException(se);
    }

    return outMap;
  }

  /**
   * Given a DOM Node object that represents a &lt;configuration&gt; block
   * within a &lt;store&gt; object, reformat this as an xml document that can be parsed
   * by {@link org.apache.hadoop.conf.Configuration}, and then return a
   * Configuration instance to pass into a KeyValueStore object to instantiate.
   *
   * @param configNode a node representing a &lt;configuration&gt; element
   *     in the DOM that is the root of the KeyValueStore's configuration.
   * @return a new Configuration containing only the key-value pairs associated
   *     with this node.
   * @throws IOException if there's an error processing the XML data.
   */
  public KeyValueStoreConfiguration parseConfiguration(Node configNode) throws IOException {
    if (null == configNode) {
      return null;
    } else if (!configNode.getNodeName().equals("configuration")) {
      throw new IOException("Expected <configuration> node, got " + configNode.getNodeName());
    }

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document document = builder.newDocument();
      Element root = document.createElement("configuration");
      document.appendChild(root);
      copyConfigNodes(root, configNode, document);

      TransformerFactory tf = TransformerFactory.newInstance();
      Transformer transformer = tf.newTransformer();
      transformer.setOutputProperty(OutputKeys.METHOD, "xml");
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      transformer.transform(new DOMSource(document), new StreamResult(outStream));

      String confXmlText = outStream.toString("UTF-8");

      // TODO(WIBI-1578): Rather than creating a brand new Configuration,
      // this method should take another to merge with.
      // This way, we can guarantee hdfs and kiji-connection resources
      // on it as well.
      Configuration conf = new Configuration(false);
      conf.addResource(new ByteArrayInputStream(confXmlText.getBytes("UTF-8")));
      return KeyValueStoreConfiguration.fromConf(conf);
    } catch (TransformerConfigurationException e) {
      throw new RuntimeException(e);
    } catch (TransformerException e) {
      throw new RuntimeException(e);
    } catch (ParserConfigurationException e) {
      throw new IOException(e);
    }
  }

  /**
   * Given a src and dest node that both represent &lt;configuration&gt;
   * elements, copy the &lt;property&gt; objects from src to dest.
   *
   * <p>The dest element is structurally modified by this operation. The
   * src argument is not modified.</p>
   *
   * <p>The &lt;name&gt; and &lt;value&gt; elements within each property
   * are copied across; other elements such as &lt;final&gt; are ignored.</p>
   *
   * <p>The text associated with each name is modified to include a "header"
   * that mirrors the KeyValueStore configuration serialization system;
   * the properties are placed in the sub-namespace of the configuration
   * associated with the '0' KeyValueStore being serialized to a Configuration
   * instance via {@link KeyValueStore#storeToConf(KeyValueStoreConfiguration)}.</p>
   *
   * @param src the input &lt;configuration&gt; element.
   * @param dest the target &lt;configuration&gt; element.
   * @param doc the target XML document.
   * @throws IOException if there is an error parsing the XML.
   */
  private static void copyConfigNodes(Element dest, Node src, Document doc) throws IOException {
    assert null != dest;
    assert null != src;

    NodeList children = src.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      if (child.getNodeType() != Node.ELEMENT_NODE) {
        continue;
      }

      if (child.getNodeName().equals("property")) {
        Node outProp = copyPropertyNode(child, doc);
        dest.appendChild(outProp);
      } else {
        throw new IOException("Unexpected element in configuration: " + child.getNodeName());
      }
    }
  }

  /**
   * Deep copies a node representing an &lt;property&gt; element.
   *
   * <p>Modifies the &lt;name&gt; element to include a header that puts the
   * property in the "namespace" of the 0 element KeyValueStore in a Configuration.</p>
   *
   * @param propertyNode the input property to clone.
   * @param doc the output XML document we're building
   * @return a Node representing the same property in the "namespace" of the 0
   * element KeyValueStore in a Configuration.
   * @throws IOException if there is an error parsing the input XML.
   */
  private static Node copyPropertyNode(Node propertyNode, Document doc) throws IOException {
    Element out = doc.createElement("property");

    NodeList propChildren = propertyNode.getChildNodes();
    for (int i = 0; i < propChildren.getLength(); i++) {
      Node child = propChildren.item(i);
      if (child.getNodeType() != Node.ELEMENT_NODE) {
        continue;
      } else if (child.getNodeName().equals("name")) {
        Element outName = doc.createElement("name");
        String inName = getChildText(child);
        outName.appendChild(doc.createTextNode(inName));
        out.appendChild(outName);
      } else if (child.getNodeName().equals("value")) {
        Element outVal = doc.createElement("value");
        outVal.appendChild(doc.createTextNode(getChildText(child)));
        out.appendChild(outVal);
      }
    }

    return out;
  }

  /**
   * Given an element with a text child, return the string contents of that
   * text child.
   *
   * @param elem the input element node.
   * @return the string contents of the single text child element.
   * @throws IOException if the XML DOM under this element is not a single text node.
   */
  private static String getChildText(Node elem) throws IOException {
    assert elem.getNodeType() == Node.ELEMENT_NODE;
    NodeList children = elem.getChildNodes();
    if (children.getLength() != 1) {
      throw new IOException("Expected exactly one text value under " + elem.getNodeName());
    }

    return children.item(0).getNodeValue();
  }
}
