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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.KijiFreshnessManager.FreshenerValidationException;
import org.kiji.scoring.KijiFreshnessManager.MultiFreshenerValidationException;
import org.kiji.scoring.KijiFreshnessManager.ValidationFailure;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.impl.InternalFreshenerContext;
import org.kiji.scoring.lib.NeverFreshen;
import org.kiji.scoring.lib.ShelfLife;

public class TestKijiFreshnessManager {
  /** A Kiji instance for testing. */
  private Kiji mKiji;
  private KijiFreshnessManager mFreshManager;

  private static final class TestScoreFunction extends ScoreFunction {
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return KijiDataRequest.create("info", "name");
    }
    public TimestampedValue<String> score(
        final KijiRowData dataToScore, final FreshenerContext context
    ) throws IOException {
      return TimestampedValue.create("new-val");
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

  private static final ShelfLife POLICY = new ShelfLife(100);
  private static final ScoreFunction SCORE_FUNCTION = new TestScoreFunction();
  private static final KijiColumnName INFO_NAME = new KijiColumnName("info", "name");
  private static final KijiColumnName INFO_EMAIL = new KijiColumnName("info", "email");
  private static final KijiColumnName INFO_INVALID = new KijiColumnName("info", "invalid");
  private static final Map<String, String> EMPTY_PARAMS = Collections.emptyMap();

  /** Tests that we can store a policy and retrieve it. */
  @Test
  public void testRegisterFreshener() throws Exception {
    mFreshManager.registerFreshener(
        "user", INFO_NAME, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    final KijiFreshenerRecord record = mFreshManager.retrieveFreshenerRecord("user", INFO_NAME);
    assertEquals(SCORE_FUNCTION.getClass().getName(), record.getScoreFunctionClass());
    assertEquals(POLICY.getClass().getName(), record.getFreshnessPolicyClass());
    final ShelfLife policyToLoad = new ShelfLife();
    policyToLoad.setup(InternalFreshenerContext.create(INFO_NAME, POLICY.serializeToParameters()));
    assertEquals(POLICY.getShelfLifeInMillis(), policyToLoad.getShelfLifeInMillis());
  }

  /** Test that we can store a policy and state using unchecked strings. */
  @Test
  public void testRegisterFreshenerWithStrings() throws Exception {
    final String policyClassString = "org.kiji.imaginary.Policy"; // Doesn't exist.
    final String scoreFunctionClassString = "org.kiji.imaginary.ScoreFunction";
    mFreshManager.registerFreshener(
        "user",
        INFO_NAME,
        policyClassString,
        scoreFunctionClassString,
        EMPTY_PARAMS,
        false,
        false,
        false);
    KijiFreshenerRecord record = mFreshManager.retrieveFreshenerRecord("user", INFO_NAME);
    assertEquals(scoreFunctionClassString, record.getScoreFunctionClass());
    assertEquals(policyClassString, record.getFreshnessPolicyClass());
  }

  /** Tests that we can store multiple policies and retrieve them. */
  @Test
  public void testRetrievePolicies() throws Exception {
    mFreshManager.registerFreshener(
        "user", INFO_NAME, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    mFreshManager.registerFreshener(
        "user", INFO_EMAIL, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    Map<KijiColumnName, KijiFreshenerRecord> records =
        mFreshManager.retrieveFreshenerRecords("user");
    assertEquals(2, records.size());
    assertTrue(records.containsKey(INFO_NAME));
    assertTrue(records.containsKey(INFO_EMAIL));
  }

  /** Tests that retrieving a policy that doesn't exist returns null. */
  @Test
  public void testEmptyRetrieve() throws Exception {
    assertNull(mFreshManager.retrieveFreshenerRecord("user", INFO_NAME));
  }

  /** Tests that we can remove policies correctly. */
  @Test
  public void testPolicyRemoval() throws Exception {
    mFreshManager.registerFreshener(
        "user", INFO_NAME, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    final KijiFreshenerRecord record = mFreshManager.retrieveFreshenerRecord("user", INFO_NAME);
    assertNotNull(record);
    mFreshManager.removeFreshener("user", INFO_NAME);
    assertNull(mFreshManager.retrieveFreshenerRecord("user", INFO_NAME));
  }

  @Test
  public void testInvalidColumnAttachment() throws IOException {
    try {
      mFreshManager.registerFreshener(
          "user", INFO_INVALID, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
      fail("registerFreshener should have thrown FreshenerValidationException because the column "
          + "does not exist.");
    } catch (FreshenerValidationException fve) {
      assertEquals(1, fve.getExceptions().size());
      assertTrue(fve.getExceptions().containsKey(ValidationFailure.NO_COLUMN_IN_TABLE));
    }

    try {
      mFreshManager.registerFreshener("user",
          new KijiColumnName("info"), POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
      fail("registerFreshener should have thrown FreshenerValidationException because the column "
          + "is not fully qualified.");
    } catch (FreshenerValidationException fve) {
      assertEquals(1, fve.getExceptions().size());
      assertTrue(
          fve.getExceptions().containsKey(ValidationFailure.GROUP_FAMILY_ATTACHMENT));
    }

    mFreshManager.registerFreshener(
        "user", INFO_NAME, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    try {
      mFreshManager.registerFreshener(
          "user", INFO_NAME, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
      fail("registerFreshener should have thrown FreshenerValidationException because there is "
          + "already a Freshener attached.");
    } catch (FreshenerValidationException fve) {
      assertEquals(1, fve.getExceptions().size());
      assertTrue(fve.getExceptions().containsKey(ValidationFailure.FRESHENER_ALREADY_ATTACHED));
    }
  }

  @Test
  public void testJavaIdentifiers() throws IOException {
    assertFalse(KijiFreshnessManager.isValidClassName("kiji..policy"));
    assertFalse(KijiFreshnessManager.isValidClassName("kiji."));
    assertFalse(KijiFreshnessManager.isValidClassName(".kiji"));
    assertTrue(KijiFreshnessManager.isValidClassName(TestScoreFunction.class.getName()));
  }

  @Test
  public void testRemovePolicies() throws IOException {
    mFreshManager.registerFreshener(
        "user", INFO_NAME, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    mFreshManager.registerFreshener(
        "user", INFO_EMAIL, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    assertEquals(Sets.newHashSet(INFO_NAME, INFO_EMAIL), mFreshManager.removeFresheners("user"));
    assertEquals(Collections.<KijiColumnName>emptySet(), mFreshManager.removeFresheners("user"));
  }

  @Test
  public void testStorePolicies() throws IOException {
    mFreshManager.registerFreshener(
        "user", INFO_NAME, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    mFreshManager.registerFreshener(
        "user", INFO_EMAIL, POLICY, SCORE_FUNCTION, EMPTY_PARAMS, false, false);
    final Map<KijiColumnName, KijiFreshenerRecord> records =
        mFreshManager.retrieveFreshenerRecords("user");
    assertEquals(2, records.size());

    final Map<KijiColumnName, KijiFreshenerRecord> modifiedRecords = Maps.newHashMap(records);
    for (Map.Entry<KijiColumnName, KijiFreshenerRecord> entry : modifiedRecords.entrySet()) {
      entry.getValue().setFreshnessPolicyClass("org.kiji.scoring.lib.NeverFreshen");
    }

    mFreshManager.registerFresheners("user", modifiedRecords, true, false, false);
    assertEquals("org.kiji.scoring.lib.NeverFreshen",
        mFreshManager.retrieveFreshenerRecord("user", INFO_NAME).getFreshnessPolicyClass());
    assertEquals("org.kiji.scoring.lib.NeverFreshen",
        mFreshManager.retrieveFreshenerRecord("user", INFO_EMAIL).getFreshnessPolicyClass());

    try {
      mFreshManager.registerFresheners("user", records, false, false, false);
      fail("storePolicies() should have thrown MultiFreshenerValidationException because of already"
          + " attached fresheners.");
    } catch (MultiFreshenerValidationException mfve) {
      final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures = mfve.getExceptions();
      assertEquals(2, failures.size());
      assertTrue(failures.containsKey(INFO_NAME));
      assertTrue(failures.containsKey(INFO_EMAIL));
      assertTrue(failures.get(INFO_NAME)
          .containsKey(ValidationFailure.FRESHENER_ALREADY_ATTACHED));
      assertTrue(failures.get(INFO_EMAIL)
          .containsKey(ValidationFailure.FRESHENER_ALREADY_ATTACHED));
    }
  }

  @Test
  public void testVersionTooLow() throws IOException {
    final KijiFreshenerRecord record = KijiFreshenerRecord.newBuilder()
        .setRecordVersion("freshenerrecord-0.0")
        .setFreshnessPolicyClass(NeverFreshen.class.getName())
        .setScoreFunctionClass(TestScoreFunction.class.getName())
        .build();
    try {
      mFreshManager.registerFreshener("user", INFO_NAME, record, false, false, false);
      fail("should have thrown FreshenerValidationException.");
    } catch (FreshenerValidationException fve) {
      assertTrue(fve.getExceptions().containsKey(ValidationFailure.VERSION_TOO_LOW));
      assertNull(mFreshManager.retrieveFreshenerRecord("user", INFO_NAME));
    }

    mFreshManager.writeRecordToMetaTable("user", INFO_NAME, record);
    assertNull(mFreshManager.retrieveFreshenerRecord("user", INFO_NAME));
  }

  @Test
  public void testVersionTooHigh() throws IOException {
    final KijiFreshenerRecord record = KijiFreshenerRecord.newBuilder()
        .setRecordVersion("freshenerrecord-1000000.0")
        .setFreshnessPolicyClass(NeverFreshen.class.getName())
        .setScoreFunctionClass(TestScoreFunction.class.getName())
        .build();
    try {
      mFreshManager.registerFreshener("user", INFO_NAME, record, false, false, false);
      fail("should have thrown FreshenerValidationException.");
    } catch (FreshenerValidationException fve) {
      assertTrue(fve.getExceptions().containsKey(ValidationFailure.VERSION_TOO_HIGH));
      assertNull(mFreshManager.retrieveFreshenerRecord("user", INFO_NAME));
    }

    mFreshManager.writeRecordToMetaTable("user", INFO_NAME, record);
    assertNull(mFreshManager.retrieveFreshenerRecord("user", INFO_NAME));
  }
}
