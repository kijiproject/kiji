/**
 * (c) Copyright 2015 WibiData, Inc.
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

package org.kiji.rest;

import java.util.Map;
import java.util.Set;

import javax.validation.Validation;
import javax.validation.Validator;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import org.junit.After;
import org.junit.Before;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * A base test class for testing Dropwizard resources. Copied from Dropwizard 0.6.2 with changes
 * made for compatibility with 0.7.x.  This is useful for writing tests without using the new
 * Dropwizard JUnit Rule helper, which does not interact well with Kiji's KijiClientTest.
 */
public abstract class ResourceTest {
  static {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  private final Set<Object> mSingletons = Sets.newHashSet();
  private final Set<Class<?>> mProviders = Sets.newHashSet();
  private final ObjectMapper mObjectMapper = new ObjectMapper();
  private final Map<String, Boolean> mFeatures = Maps.newHashMap();
  private final Map<String, Object> mProperties = Maps.newHashMap();

  private JerseyTest mTest;
  private Validator mValidator = Validation.buildDefaultValidatorFactory().getValidator();

  protected abstract void setUpResources() throws Exception;

  public Validator getValidator() {
    return mValidator;
  }

  public void setValidator(Validator validator) {
    this.mValidator = validator;
  }

  protected void addResource(Object resource) {
    mSingletons.add(resource);
  }

  public void addProvider(Class<?> klass) {
    mProviders.add(klass);
  }

  public void addProvider(Object provider) {
    mSingletons.add(provider);
  }

  protected ObjectMapper getObjectMapper() {
    return mObjectMapper;
  }

  protected void addFeature(String feature, Boolean value) {
    mFeatures.put(feature, value);
  }

  protected void addProperty(String property, Object value) {
    mProperties.put(property, value);
  }

  protected Client client() {
    return mTest.client();
  }

  protected JerseyTest getJerseyTest() {
    return mTest;
  }

  @Before
  public final void setUpJersey() throws Exception {
    setUpResources();
    this.mTest = new JerseyTest() {
      @Override
      protected AppDescriptor configure() {
        final DropwizardResourceConfig config =
            DropwizardResourceConfig.forTesting(new MetricRegistry());
        for (Class<?> provider : mProviders) {
          config.getClasses().add(provider);
        }
        for (Map.Entry<String, Boolean> feature : mFeatures.entrySet()) {
          config.getFeatures().put(feature.getKey(), feature.getValue());
        }
        for (Map.Entry<String, Object> property : mProperties.entrySet()) {
          config.getProperties().put(property.getKey(), property.getValue());
        }
        config.getSingletons().add(new JacksonMessageBodyProvider(getObjectMapper(), mValidator));
        config.getSingletons().addAll(mSingletons);
        return new LowLevelAppDescriptor.Builder(config).build();
      }
    };
    mTest.setUp();
  }

  @After
  public final void tearDownJersey() throws Exception {
    if (mTest != null) {
      mTest.tearDown();
    }
  }
}
