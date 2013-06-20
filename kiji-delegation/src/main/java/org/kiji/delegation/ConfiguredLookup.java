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

package org.kiji.delegation;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * Provider of the {@link Lookup} API that relies on configuration files to
 * specify the implementation of each interface to use.
 *
 * <p>This Lookup instance will search through all classpath resources named
 * <tt>kiji-delegation.properties</tt> to find the correct implementation to use
 * for a given interface.</p>
 *
 * <p>The <tt>kiji-delegation.properties</tt> file is a Java properties file
 * that can be parsed by {@link java.util.Properties}. It specifies a number
 * of lines of the form:</p>
 *
 * <div><tt><pre>com.example.FooInterface=com.example.impl.FooImpl
 * com.example.BarInterface=com.example.impl.BarImpl
 * ...</pre></tt></div>
 *
 * <p>A <tt>ConfiguredLookup</tt> intended to produce instances of <tt>FooInterface</tt>
 * will search through all <tt>kiji-delegation.properties</tt> resources available
 * on the classpath until it finds the first available binding. It then tries to
 * create a new instance of the specified interface. If this instance cannot be created,
 * it reverts to using a <tt>BasicLookup</tt> to check for a binding specified in the
 * <tt>META-INF/services/</tt> resources of the jar files on the classpath via
 * {@link java.util.ServiceLoader}.</p>
 *
 * <p>The default implementation of an interface preferred by a jar should be
 * specified through <tt>META-INF/services/</tt>. The <tt>kiji-delegation.properties</tt>
 * file should be specified only by clients as a means of reconfiguring the jar.</p>
 *
 * <p>In this context, "interface" could refer to a Java interface, abstract class, or even
 * a concrete parent class.</p>
 *
 * <p>This implementation caches maps constructed by parsing
 * <tt>kiji-delegation.properties</tt> files. If multiple <tt>kiji-delegation.properties</tt>
 * files exist on the classpath, mappings in files that appear earlier have precedence. Cached
 * maps are maintained on a per-ClassLoader basis.  You can call {@link #clearCache} to clear
 * the resolution cache; for example, if you have modified the set of jars available on the
 * classpath to a given ClassLoader.</p>
 *
 * @param <T> the type that this Lookup instance provides.
 */
@ApiAudience.Public
public final class ConfiguredLookup<T> extends Lookup<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ConfiguredLookup.class);

  /** The name of the resource file that contains the lookup binding info. */
  public static final String CONF_RESOURCE_NAME = "kiji-delegation.properties";

  /**
   * A cache of parsed kiji-delegation.properties files, maintained on a per-ClassLoader basis.
   * Access to this map is guaranteed to be thread safe.
   **/
  private static final Map<ClassLoader, Properties> RESOLUTION_CACHE =
      Collections.synchronizedMap(new WeakHashMap<ClassLoader, Properties>());


  /** The ClassLoader that we use to look up service implementations. */
  private final ClassLoader mClassLoader;
  /** A representation of the interface or abstract class the user wants to load. */
  private final Class<T> mClass;

  /**
   * A BasicLookup instance that is used as a fallback for default configuration if no
   * user-supplied configuration is available for the class.
   */
  private final Lookup<T> mBasicLookup;

  /**
   * Create a ConfiguredLookup instance. Package-private c'tor; clients should use the
   * Lookup.get() factory methods.
   *
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use.
   */
  ConfiguredLookup(Class<T> clazz, ClassLoader classLoader) {
    assert null != clazz;
    assert null != classLoader;
    mClass = clazz;
    mClassLoader = classLoader;
    mBasicLookup = Lookups.get(clazz, classLoader);
  }

  /**
   * Lookup the configured provider instance for the specified class and return
   * a new instance. This first looks to the property line in the kiji-delegation.properties
   * resource; if none is present, it uses a BasicLookup to find an available service
   * class and provides a new instance of that class.
   *
   * @return the configured provider instance for the class, or one found through a BasicLookup
   *     if no such configuration is present.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found.
   */
  @Override
  public T lookup() {
    Properties properties = getPropertiesForClassLoader();

    String implementationName = (String) properties.get(mClass.getName());
    if (null == implementationName) {
      // Use BasicLookup to get the name of the class representing the fallback implementation
      // to use. BasicLookup will repeatedly return the same instance; we use this mechanism
      // to create a new instance every time.
      implementationName = mBasicLookup.lookup().getClass().getName();
      // This will throw NoSuchProviderException rather than return null.
    }

    try {
      Class<T> implClass = (Class<T>) Class.forName(implementationName, true, mClassLoader);
      return (T) implClass.newInstance();
    } catch (IllegalAccessException iae) {
      throw new NoSuchProviderException("Could not instantiate " + implementationName, iae);
    } catch (InstantiationException ie) {
      throw new NoSuchProviderException("Could not instantiate " + implementationName, ie);
    } catch (ClassNotFoundException cnfe) {
      throw new NoSuchProviderException("No such implementation [" + implementationName
          + "] for interface " + mClass.getName());
    }
  }

  /**
   * Lookup the configured provider instance for the specified class and return
   * a new instance. This first looks to the property line in the kiji-delegation.properties
   * resource; if none is present, it uses a BasicLookup to find an available service
   * class and provides a new instance of that class.
   *
   * @param runtimeHints ignored.
   * @return the configured provider instance for the class, or one found through a BasicLookup
   *     if no such configuration is present.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found.
   */
  @Override
  public T lookup(Map<String, String> runtimeHints) {
    LOG.debug("ConfiguredLookup.lookup() called with runtimeHints; ignoring them.");
    return lookup();
  }

  /**
   * Unsupported operation.
   *
   * @throws UnsupportedOperationException - this method is not implemented on ConfiguredLookup.
   * @return nothing.
   */
  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException("ConfiguredLookup does not support iteration");
  }

  /**
   * Unsupported operation.
   *
   * @param runtimeHints ignored.
   * @throws UnsupportedOperationException - this method is not implemented on ConfiguredLookup.
   * @return nothing.
   */
  @Override
  public Iterator<T> iterator(Map<String, String> runtimeHints) {
    throw new UnsupportedOperationException("ConfiguredLookup does not support iteration");
  }

  /**
   * Returns the Properties object representing the configuration map to use with this
   * ClassLoader.
   *
   * <p>If one is present in RESOLUTION_CACHE already, then this one is used. Otherwise,
   * this will parse a new Properties object and use and install that one.</p>
   *
   * <p>If a resource cannot be parsed due to an IOException, this causes a
   *
   * @return the Properties object representing the implementation-to-interface bindings
   *     to use with this ClassLoader.
   */
  private Properties getPropertiesForClassLoader() {
    Properties cachedProps = RESOLUTION_CACHE.get(mClassLoader);
    if (null != cachedProps) {
      // Cache hit - just return this one.
      return cachedProps;
    }

    // Walk through the available kiji-delegation.properties files and parse them.
    Properties confProps = new Properties();
    try {
      Enumeration<URL> resources = mClassLoader.getResources(CONF_RESOURCE_NAME);
      if (null == resources) {
        LOG.debug("No kiji-delegation.properties file was found on the classpath, but interface");
        LOG.debug(mClass.getName() + " is being loaded through a ConfiguredLookup. This will use");
        LOG.debug("the default implementation, if any, in the underlying classpath.");
        return cacheProperties(confProps);
      }
      while (resources.hasMoreElements()) {
        URL resourceUrl = resources.nextElement();
        try {
          InputStream resourceStream = resourceUrl.openStream();
          Properties resourceProps = new Properties();
          try {
            resourceProps.load(resourceStream);
          } finally {
            resourceStream.close();
          }

          // Do not allow "later" classpath entries to overwrite earlier entries;
          // merge the resourceProps into confProps, but don't overwrite entries.
          for (Map.Entry<Object, Object> resourceEntry : resourceProps.entrySet()) {
            String key = (String) resourceEntry.getKey();
            String val = (String) resourceEntry.getValue();
            if (!confProps.containsKey(key)) {
              confProps.put(key, val);
            } else {
              LOG.debug("Multiple copies of kiji-delegation.properties on the classpath define");
              LOG.debug("a binding for interface " + mClass.getName());
              LOG.debug("Using implementation: " + confProps.get(key));
            }
          }
        } catch (IOException ioe) {
          LOG.error("Could not load kiji-delegation configuration data from classpath entry: "
             + resourceUrl + ". Some implementation bindings may not be present.");
        }
      }
    } catch (IOException ioe) {
      // IOException in ClassLoader.getResources()
      LOG.error("Could not iterate over elements of the classpath to load " + CONF_RESOURCE_NAME);
      LOG.error("Got IOException: " + ioe.getMessage());
      LOG.error("Will revert to using default implementation bindings.");
    }

    // Install confProps in RESOLUTION_CACHE and return it.
    return cacheProperties(confProps);
  }

  /**
   * Cache the specified Properties instance as belonging to this ClassLoader. Double check
   * that one is not already cached.
   *
   * @param properties the object to cache for this Lookup's ClassLoader.
   * @return the Properties object to actually use: typically this one, but if another thread
   *     has cached a different Properties object for the same ClassLoader, use that one.
   */
  private Properties cacheProperties(Properties properties) {
    // Install properties in RESOLUTION_CACHE and return it.
    // Note that we may have raced with another thread to parse properties for this
    // ClassLoader. Double check that this is not the case.
    synchronized (RESOLUTION_CACHE) {
      if (RESOLUTION_CACHE.containsKey(mClassLoader)) {
        // Another thread already parsed properties for this ClassLoader.
        Properties cachedProps = RESOLUTION_CACHE.get(mClassLoader);
        // Check for null again since it's a weak HashMap. Even under this synchronized
        // block there's still a race with the gc.
        if (null == cachedProps) {
          RESOLUTION_CACHE.put(mClassLoader, properties);
          return properties;
        } else {
          return cachedProps;
        }
      } else {
        RESOLUTION_CACHE.put(mClassLoader, properties);
        return properties;
      }
    }
  }
}
