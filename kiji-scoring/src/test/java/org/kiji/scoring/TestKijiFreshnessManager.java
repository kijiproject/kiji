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

package org.kiji.scoring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;
import org.kiji.scoring.lib.ShelfLife;

/**
 * Created with IntelliJ IDEA. User: aaron Date: 4/16/13 Time: 10:13 AM To change this template use
 * File | Settings | File Templates.
 */
public class TestKijiFreshnessManager {
  /** A Kiji instance for testing. */
  private Kiji mKiji;
  private KijiFreshnessManager mFreshManager;

  private static final class TestProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info", "name");
    }
    public String getOutputColumn() {
      return "info:name";
    }
    public void produce(
        final KijiRowData kijiRowData, final ProducerContext producerContext) throws IOException {
      producerContext.put("new-val");
    }
  }

  @Before
  public void setup() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.USER_TABLE));

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("user", layout)
          .withRow("foo")
            .withFamily("info")
              .withQualifier("name").withValue(5L, "foo-val")
              .withQualifier("email").withValue(5L, "foo@bar.org")
          .withRow("bar")
             .withFamily("info")
                .withQualifier("name").withValue(5L, "bar-val")
                .withQualifier("email").withValue(5L, "bar@foo.org")
        .build();

    // Fill local variables.
    mFreshManager = KijiFreshnessManager.create(mKiji);
  }

  @After
  public void cleanup() throws Exception {
    mFreshManager.close();
    mKiji.release();
  }

  /** Tests that we can store a policy and retrieve it. */
  @Test
  public void testStorePolicy() throws Exception {
    ShelfLife policy = new ShelfLife(100);
    mFreshManager.storePolicy("user", "info:name", TestProducer.class, policy);
    KijiFreshnessPolicyRecord mPolicyRecord = mFreshManager.retrievePolicy("user", "info:name");
    assertEquals(mPolicyRecord.getProducerClass(), TestProducer.class.getName());
    assertEquals(mPolicyRecord.getFreshnessPolicyClass(), ShelfLife.class.getName());
    ShelfLife policyLoaded = new ShelfLife();
    policyLoaded.deserialize(mPolicyRecord.getFreshnessPolicyState());
    assertEquals(policy.getShelfLifeInMillis(), policyLoaded.getShelfLifeInMillis());
  }

  /** Test that we can store a policy and state using unchecked strings. */
  @Test
  public void testStorePolicyWithStrings() throws Exception {
    String policyClassString = "org.kiji.imaginary.Policy"; // Doesn't exist.
    String policyState = "SomeState";
    String producerClassString = "org.kiji.imaginary.Producer";
    mFreshManager.storePolicyWithStrings(
        "user", "info:name", producerClassString, policyClassString, policyState);
    KijiFreshnessPolicyRecord mPolicyRecord = mFreshManager.retrievePolicy("user", "info:name");
    assertEquals(mPolicyRecord.getProducerClass(), producerClassString);
    assertEquals(mPolicyRecord.getFreshnessPolicyClass(), policyClassString);
    assertEquals(mPolicyRecord.getFreshnessPolicyState(), policyState);
  }

  /** Tests that we can store multiple policies and retrieve them. */
  @Test
  public void testRetrievePolicies() throws Exception {
    ShelfLife policy = new ShelfLife(100);
    mFreshManager.storePolicy("user", "info:name", TestProducer.class, policy);
    mFreshManager.storePolicy("user", "info:email", TestProducer.class, policy);
    mFreshManager.storePolicy("user", "networks", TestProducer.class, policy);
    Map<KijiColumnName, KijiFreshnessPolicyRecord> policies =
        mFreshManager.retrievePolicies("user");
    assertEquals(policies.size(), 3);
    assertTrue(policies.containsKey(new KijiColumnName("info", "name")));
    assertTrue(policies.containsKey(new KijiColumnName("info", "email")));
    assertTrue(policies.containsKey(new KijiColumnName("networks")));
  }

  /** Tests that retrieving a policy that doesn't exist returns null. */
  @Test
  public void testEmptyRetrieve() throws Exception {
    assertNull(mFreshManager.retrievePolicy("user", "info:name"));
  }

  /** Tests that we can remove policies correctly. */
  @Test
  public void testPolicyRemoval() throws Exception {
    ShelfLife policy = new ShelfLife(100);
    mFreshManager.storePolicy("user", "info:name", TestProducer.class, policy);
    KijiFreshnessPolicyRecord mPolicyRecord = mFreshManager.retrievePolicy("user", "info:name");
    assertNotNull(mPolicyRecord);
    mFreshManager.removePolicy("user", "info:name");
    mPolicyRecord = mFreshManager.retrievePolicy("user", "info:name");
    assertNull(mPolicyRecord);
  }

  @Test
  public void testInvalidColumnAttachment() throws IOException {
    final ShelfLife policy = new ShelfLife(100);
    try {
      mFreshManager.storePolicy("user", "info:invalid", TestProducer.class, policy);
    } catch (IllegalArgumentException iae) {
      assertEquals("Table does not contain specified column: info:invalid", iae.getMessage());
    }
    try {
      mFreshManager.storePolicy("user", "info", TestProducer.class, policy);
    } catch (IllegalArgumentException iae) {
      assertEquals("Specified family: info is not a valid Map Type family in the table: user",
          iae.getMessage());
    }
    mFreshManager.storePolicy("user", "networks", TestProducer.class, policy);
    try {
      mFreshManager.storePolicy("user", "networks:qualifier", TestProducer.class, policy);
    } catch (IllegalArgumentException iae) {
      assertEquals("There is already a freshness policy attached to family: networks Freshness "
          + "policies may not be attached to a map type family and fully qualified columns within "
          + "that family.", iae.getMessage());
    }
    mFreshManager.removePolicy("user", "networks");
    mFreshManager.storePolicy("user", "networks:qualifier", TestProducer.class, policy);
    try {
      mFreshManager.storePolicy("user", "networks", TestProducer.class, policy);
    } catch (IllegalArgumentException iae) {
      assertEquals("There is already a freshness policy attached to a fully qualified column in "
          + "family: networks Freshness policies may not be attached to a map type family and fully"
          + " qualified columns within that family. To view a list of attached freshness policies "
          + "check log files for KijiFreshnessManager.", iae.getMessage());
    }
  }
}
