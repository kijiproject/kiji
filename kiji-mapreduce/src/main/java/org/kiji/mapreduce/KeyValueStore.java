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

package org.kiji.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.kiji.mapreduce.kvstore.XmlKeyValueStoreParser;

/**
 * <p>A KeyValueStore specifies all the resources needed to surface
 * key-value pairs from some backing store.  This may be defined on
 * files, a Kiji table, or some other resource.
 * See the org.kiji.mapreduce.kvstore package for implementations.</p>
 *
 * <p>Within the Kiji framework, a KeyValueStore may be used within a MapReduce job,
 * or outside of one, in a Freshener.  In a MapReduce job, it will have access
 * to the job Configuration.  When run inside a Freshener, it will only have
 * Kiji connection resources.
 * As a result, you should be careful that your KeyValueStore implementation
 * can be run with the resources available in either environment.
 * (For example, you should <i>not</i> use the DistributedCache
 * in <code>storeToConf()</code> when a KeyValueStore is used within a Freshener)</p>
 *
 * <h1>Lifecycle in the Kiji ecosystem:</h1>
 * <p>A KeyValueStore is bound to a name by the KeyValueStoreClient.getRequiredStores() method.
 * At runtime, you may override these name-to-KeyValueStore bindings
 * by specifying xml configuration files.  See
 * "Overriding KeyValueStore Definitions at Run-time" in the quick-start docs for details.
 * These KeyValueStores will be instantiated with the default constructor,
 * and initialized with the <code>configureFromXml()</code> method.
 * Note that this does <i>not</i> have access to the job Configuration,
 * regardless of whether this KeyValueStore is being used within a
 * MapReduce job.</p>
 *
 * <p>In a MapReduce job, it will be serialized into a unique namespace of the
 * job Configuration by calling <code>storeToConf()</code>.
 * On the mapper side, this KeyValueStore will be deserialized by calling the
 * default constructor (so it's important that you specify one),
 * then initialized with a single call to <code>initFromConf()</code>.</p>
 *
 * <p>To actually read key-value pairs, get a KeyValueStoreReader with the
 * <code>open()</code> method.  Close it when you're finished using it.</p>
 *
 * @param <K> the key type expected to be implemented by the keys to this store.
 * @param <V> the value type expected to be accessed by the keys to this store.
 */
// TODO(WIBI-1632): configureFromXml() should be removed.
// TODO(WIBI-1578): The job Configuration should either *always*
// be present at initialization, or else never present then.
// If the later, then perhaps KeyValueStore.open() can take the job Configuration?
public abstract class KeyValueStore<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueStore.class.getName());

  /**
   * KeyValueStore definitions are serialized to the Configuration as a set of keys under
   * the "kiji.produce.kvstore.[i]." namespace.
   */
  // TODO(WIBI-1635): Remove this variable, and update unit tests to no longer rely on it.
  public static final String CONF_KEY_VALUE_BASE = "kiji.produce.kvstore.";

  /** KeyValueStoreConfiguration variable storing the class name to deserialize the store. */
  public static final String CONF_CLASS = "class";

  /** KeyValueStoreConfiguratoin variable storing the name of this KeyValueStore. */
  public static final String CONF_NAME = "name";

  /** How many KeyValueStore definitions were serialized in the configuration? */
  public static final String CONF_KEY_VALUE_STORE_COUNT = "kiji.produce.kvstore.count";

  /** Default value for CONF_KEY_VALUE_STORE_COUNT. */
  public static final int DEFAULT_KEY_VALUE_STORE_COUNT = 0;

  /** Construct a new KeyValueStore. */
  public KeyValueStore() {
  }

  /**
   * Override to serialize the state of your KeyValueStore into
   * the provided KeyValueStoreConfiguration.
   *
   * @param conf The KeyValueStoreConfiguration to serialize state into.
   *     This will be placed in a unique namespace of the job Configuration,
   *     so you have carte blanche to write any key here.
   * @throws IOException if there is an error writing state to the configuration.
   */
  public abstract void storeToConf(KeyValueStoreConfiguration conf) throws IOException;

  /**
   * Override to deserialize the state of this KeyValueStore from the
   * KeyValueStoreConviguration.
   *
   * @param conf The KeyValueStoreConfiguration storing state for this KeyValueStore.
   * @throws IOException if there is an error reading from the configuration.
   */
  public abstract void initFromConf(KeyValueStoreConfiguration conf) throws IOException;

  /**
   * Opens an instance of this KeyValueStore for access by clients.
   *
   * @return the KeyValueStoreReader associated with this KeyValueStore.
   * @throws IOException if there is an error opening the underlying storage resource.
   * @throws InterruptedException if there is an interruption while communicating with
   *     the underlying storage resource.
   */
  public abstract KeyValueStoreReader<K, V> open() throws IOException, InterruptedException;

  /**
   * Allows a store to define a mechanism for reading arbitrary serialized data from an
   * XML file specifying KeyValueStore definitions.
   *
   * <p>KeyValueStore definitions may be specified in an XML file applied by the user.
   * Each store is defined in a &lt;store&gt; element. Many KeyValueStore implementations
   * expect the store element to contain a single child element called &lt;configuration&gt;.
   * This default implementation of configureFromXml() will read this child element
   * as a KeyValueStoreConfiguration object, then initialize this KeyValueStore by passing
   * this to initFromConf().</p>
   *
   * <p>If no &lt;configuration&gt; element is present, the KeyValueStore is initialized
   * with an empty KeyValueStoreConfiguration.</p>
   *
   * <p>Overriding this method allows a KeyValueStore instance to perform more involved
   * parsing, should the store support this interface. Refer to individual store
   * implementations to determine what child elements, if any, they support.</p>
   *
   * @param xmlNode the w3c DOM node representing the &lt;store&gt; element in the document.
   * @param storeName the name being bound to this store instance.
   * @throws IOException if there is an error parsing the XML document node.
   */
  public void configureFromXml(Node xmlNode, String storeName) throws IOException {
    NodeList storeChildren = xmlNode.getChildNodes();
    Node configurationNode = null;
    int numRealChildren = 0;
    for (int j = 0; j < storeChildren.getLength(); j++) {
      Node storeChild = storeChildren.item(j);
      if (storeChild.getNodeType() != Node.ELEMENT_NODE) {
        continue;
      } else {
        numRealChildren++;
        if (storeChild.getNodeName().equals("configuration")) {
          configurationNode = storeChild;
        }
      }
    }

    if (numRealChildren > 1) {
      // Don't recognize the XML schema here.
      throw new IOException("Unrecognized XML schema for store " + storeName
          + "; expected <configuration> element.");
    } else if (numRealChildren == 0) {
      assert null == configurationNode;
      LOG.warn("No <configuration> supplied for store " + storeName);
      initFromConf(KeyValueStoreConfiguration.fromConf(new Configuration(false)));
    } else if (null == configurationNode) {
      // Got a single child element, but it wasn't a <configuration>.
      throw new IOException("Unrecognized XML schema for store " + storeName
          + "; expected <configuration> element.");
    } else {
      assert null != configurationNode;
      assert numRealChildren == 1;
      // Configure the store by parsing the <configuration> element.
      KeyValueStoreConfiguration conf = new XmlKeyValueStoreParser()
          .parseConfiguration(configurationNode);
      initFromConf(conf);
    }
  }

  @Override
  public String toString() {
    return getClass().getName();
  }

  /**
   * Fills any unbound stores in <code>storeBindings</code> with the default
   * implementations found in <code>requiredStores</code>.
   *
   * <p>Each store in <code>requiredStores</code> must have either:</p>
   * <ul>
   *   <li>A default implementation, or</li>
   *   <li>An implementation defined an <code>storeBindings</code>.</li>
   * </ul>
   *
   * <p>If not, an exception is thrown.</p>
   *
   * @param requiredStores A map of required store names to their default implementations
   *     (may be null).
   * @param storeBindings A map of bound stores, which will be modified to contain default
   *     implementations from <code>requiredStores</code> if not yet bound.
   * @throws IOException If any of the required stores do not validate.
   */
  public static final void bindAndValidateRequiredStores(
      Map<String, KeyValueStore<?, ?>> requiredStores,
      Map<String, KeyValueStore<?, ?>> storeBindings) throws IOException {
    if (null == requiredStores) {
      // No required stores. Nothing to validate.
      return;
    }

    // Check that each required store was either bound or had a default.
    for (Map.Entry<String, KeyValueStore<?, ?>> entry : requiredStores.entrySet()) {
      final String requiredName = entry.getKey();
      final KeyValueStore<?, ?> defaultImplementation = entry.getValue();

      if (null != storeBindings.get(requiredName)) {
        // There was an implementation bound to this required store, no whammy.
        continue;
      }
      if (null != defaultImplementation) {
        // There was a default implementation for this required store, no whammy.
        storeBindings.put(requiredName, defaultImplementation);
        continue;
      }

      // Whammy! No default implementation and no user-bound implementation.
      throw new IOException("Store named " + requiredName + " is required, but not defined.");
    }
  }

  /**
   * Given a set of KeyValueStores, add their configuration data to the provided Configuration.
   * This replaces any previous KeyValueStore configs in the Configuration.
   *
   * @param stores the map from names to KeyValueStores to serialize.
   * @param conf the Configuration to hold the stores.
   * @throws IOException if there is an error serializing the configuration of a store.
   */
  public static final void addStoreMapToConfiguration(Map<String, KeyValueStore<?, ?>> stores,
      Configuration conf) throws IOException {
    int i = 0;
    for (Map.Entry<String, KeyValueStore<?, ?>> entry : stores.entrySet()) {
      String storeName = entry.getKey();
      KeyValueStore<?, ?> store = entry.getValue();

      if (null != store) {
        KeyValueStoreConfiguration kvStoreConf = new KeyValueStoreConfiguration(conf, i);

        // Set the default deserializer class to the same one that serialized it.
        // This is done first, so the store itself can override it.
        kvStoreConf.set(KeyValueStore.CONF_CLASS, store.getClass().getName());

        // Serialize the class' inner data itself.
        store.storeToConf(kvStoreConf);

        // Then store the name binding associated with this KeyValueStore.
        //
        // TODO(WIBI-1534): This can clobber a value that the user has set.
        // This should be stored in a different namespace.
        kvStoreConf.set(KeyValueStore.CONF_NAME, storeName);

        i++;
      }
    }

    // Record the number of KeyValueStore instances we wrote to the conf, so we can
    // deserialize the same number afterward.
    conf.setInt(KeyValueStore.CONF_KEY_VALUE_STORE_COUNT, i);
  }
}
