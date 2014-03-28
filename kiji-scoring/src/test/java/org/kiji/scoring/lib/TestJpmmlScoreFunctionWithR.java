/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.scoring.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.*;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.Resources;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.avro.*;


/**
 * Test all of the various R models with the JPMML score function.
 */
public class TestJpmmlScoreFunctionWithR extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestJpmmlScoreFunctionWithR.class);

  private static final String TABLE_NAME = "table";
  private static final String ROW_KEY = "foo";

  /** Common IrisData to use for all (most) of the Iris tests. */
  private static AuditData mAuditData;
  private static IrisData mIrisData;
  private static OzoneData mOzoneData;
  private static GenericRecord mAuditResult;
  private static GenericRecord mIrisResult;

  /**
   * Set up actual test vectors and expected results from the JPMML source code (within
   * pmml-rattle).  The test vectors should usually be from the first lines of the input/output
   * CSVs for the respective tests.
   */
  @BeforeClass
  public static void initTestData() {

    // ---------------------------------------------------------------------------------------------
    // Audit stuff.

    mAuditData = AuditData.newBuilder()
        .setEmployment("Private")
        .setEducation("College")
        .setMarital("Unmarried")
        .setOccupation("Service")
        .setIncome(81838)
        .setGender("Female")
        .setDeductions(0)
        .setHours(72)
        .setAdjusted("0")
        .build();

    mAuditResult = AuditResult.newBuilder().setPredictedAdjusted("0").build();

    // ---------------------------------------------------------------------------------------------
    // Iris stuff.

    mIrisData = IrisData.newBuilder()
        .setSepalLength(5.1)
        .setSepalWidth(3.5)
        .setPetalLength(1.4)
        .setPetalWidth(0.2)
        .setSpecies("setosa")
        .build();

    mIrisResult = IrisResult.newBuilder().setPredictedSpecies("setosa").build();

    // ---------------------------------------------------------------------------------------------
    // Ozone stuff.

    mOzoneData = OzoneData.newBuilder()
        .setO3(3)
        .setTemp(40)
        .setIbh(2693)
        .setIbt(87)
        .build();
  }

  /*
  // TODO (SCORE-173): Figure out how to handle a score function that should take a collection!
  @Test
  public void testAssociationRulesShoppingData() throws IOException {
    runRTest(
        AssociationRulesShoppingData.newBuilder()
            .setProduct("softdrink")
            .setTransaction("1234")
            .build(),
        "AssociationRulesShopping.pmml",
        "shopping"
    );
  }
  */

  @Test
  public void testDecisionTreeIris() throws IOException {
    runRTest(mIrisData, "DecisionTreeIris.pmml", "RPart_Model", mIrisResult);
  }

  @Test
  public void testGeneralRegressionAudit() throws IOException {
    runRTest(mAuditData, "GeneralRegressionAudit.pmml", "General_Regression_Model", mAuditResult);
  }

  @Test
  public void testGeneralRegressionIris() throws IOException {
    runRTest(
        mIrisData,
        "GeneralRegressionIris.pmml",
        "General_Regression_Model",
        IrisResultGeneralRegression.newBuilder()
            .setPredictedVersicolor("0")
            .setProbability1(0.1451)
            .build()
    );
  }

  @Test
  public void testGeneralRegressionOzone() throws IOException {
    runRTest(
        mOzoneData,
        "GeneralRegressionOzone.pmml",
        "General_Regression_Model",
        OzoneResult.newBuilder().setPredictedO3(5.1525).build()
    );
  }

  // TODO: Not sure how to score a clustering algorithm...
  // This test does weird stuff!
  // This returns a result with a null field name...
  /*
  @Test
  public void testHierarchicalClusteringIris() throws IOException {
    runRTest(
        IrisData.newBuilder()
            .setPetalLength(1.0)
            .setPetalWidth(1.0)
            .setSepalLength(1.0)
            .setSepalWidth(1.0)
            .setSpecies("Trying to predict this!")
            .build(),
        "HierarchicalClusteringIris.pmml",
        "HClust_Model"
    );
  }
  */

  // TODO: Not sure how to score a clustering algorithm...
  // This test does weird stuff!
  // This returns a result with a null field name...
  /*
  @Test
  public void testKMeansIris() throws IOException {
    runRTest(
        IrisData.newBuilder()
            .setPetalLength(1.0)
            .setPetalWidth(1.0)
            .setSepalLength(1.0)
            .setSepalWidth(1.0)
            .setSpecies("Trying to predict this!")
            .build(),
        "KMeansIris.pmml",
        "KMeans_Model"
    );
  }
  */

  @Test
  public void testNaiveBayesAudit() throws IOException {
    runRTest(
        mAuditData,
        "NaiveBayesAudit.pmml",
        "naiveBayes_Model",
        AuditResultWithProbability.newBuilder()
            .setPredictedAdjusted("0")
            .setProbability0(0.9975)
            .setProbability1(0.00245)
            .build()
    );
  }

  @Test
  public void testNaiveBayesIris() throws IOException {
    runRTest(
        IrisDataNaiveBayes.newBuilder()
            .setSepalLength(5.1)
            .setSepalWidth(3.5)
            .setPetalLength(1.4)
            .setPetalWidth(0.2)
            .setSpecies("setosa")
            // TODO: Figure out if the user expects to have to set this weird field.
            .setDiscretePlaceHolder("pseudoValue") // no idea what this does...
            .build(),
        "NaiveBayesIris.pmml",
        "naiveBayes_Model",
        mIrisResult
    );
  }

  @Test
  public void testNeuralNetworkAudit() throws IOException {
    runRTest(mAuditData, "NeuralNetworkAudit.pmml", "NeuralNet_model", mAuditResult);
  }

  @Test
  public void testNeuralNetworkIris() throws IOException {
    runRTest(mIrisData, "NeuralNetworkIris.pmml", "NeuralNet_model", mIrisResult);
  }

  @Test
  public void testNeuralNetworkOzone() throws IOException {
    runRTest(mOzoneData, "NeuralNetworkOzone.pmml", "NeuralNet_model",
        OzoneResult.newBuilder().setPredictedO3(4.60137).build());
  }

  @Test
  public void testRandomForestAudit() throws IOException {
    runRTest(mAuditData, "RandomForestAudit.pmml", "randomForest_Model", mAuditResult);
  }

  @Test
  public void testRandomForestIris() throws IOException {
    runRTest(mIrisData, "RandomForestIris.pmml", "randomForest_Model", mIrisResult);
  }

  @Test
  public void testRandomForestOzone() throws IOException {
    runRTest(mOzoneData, "RandomForestOzone.pmml", "randomForest_Model",
        OzoneResult.newBuilder().setPredictedO3(4.99126).build());
  }

  @Test
  public void testRegressionIris() throws IOException {
    runRTest(
        mIrisData,
        "RegressionIris.pmml",
        "multinom_Model",
        IrisResultWithProbability.newBuilder()
            .setPredictedSpecies("setosa")
            .setProbabilitySetosa(0.9999)
            .setProbabilityVersicolor(1.52640620079599e-09)
            .setProbabilityVirginica(2.71641729324424e-36)
            .build()
    );
  }

  @Test
  public void testRegressionOzone() throws IOException {
    runRTest(mOzoneData, "RegressionOzone.pmml", "Linear_Regression_Model",
        OzoneResult.newBuilder().setPredictedO3(3.78867).build());
  }

  @Test
  public void testSupportVectorMachineAudit() throws IOException {
    runRTest(mAuditData, "SupportVectorMachineAudit.pmml", "SVM_model", mAuditResult);
  }

  @Test
  public void testSupportVectorMachineIris() throws IOException {
    runRTest(mIrisData, "SupportVectorMachineIris.pmml", "SVM_model", mIrisResult);
  }

  @Test
  public void testSupportVectorMachineOzone() throws IOException {
    runRTest(mOzoneData, "SupportVectorMachineOzone.pmml", "SVM_model",
        OzoneResult.newBuilder().setPredictedO3(4.29).build());
  }

  @Test
  public void testDecisionTreeAuditIllegalData() throws IOException {
    // Test the probabilities as well as "Adjusted."
    AuditData auditData = AuditData.newBuilder()
        .setEmployment("Computer hacker!")
        .setEducation("College")
        .setMarital("Unmarried")
        .setOccupation("Service")
        .setIncome(81838)
        .setGender("Female")
        .setDeductions(0)
        .setHours(72)
        .setAdjusted("0")
        .build();
    try {
      runRTest(auditData, "DecisionTreeAudit.pmml", "RPart_Model", mAuditResult);
      Assert.fail("Should have failed on an exception for an illegal value for Employment.");
    } catch (Exception e) {
      assert(e.toString().contains("Trouble setting field"));
    }
  }

  @Test
  public void testIllegalDataType() throws IOException {
    try {
      runRTest(
          OzoneDataWrongType.newBuilder()
              .setO3(3)
              .setTemp("hot!")
              .setIbh(2693)
              .setIbt(87)
              .build(),
          "SupportVectorMachineOzone.pmml",
          "SVM_model",
          OzoneResult.newBuilder().setPredictedO3(4.29).build()
      );
      Assert.fail("Should have failed on an exception for an illegal data type for Temp.");
    } catch (Exception e) {
      assert(e.toString().contains("Trouble setting field")) : e.toString();
    }
  }

  /**
   * Runs the JPMML score function against R PMML model and compares to a known correct result.
   *
   * @param predictor Input data to the score function.
   * @param pmmlFileName The name of the PMML XML file.
   * @param pmmlModelName The name of the model within the PMML XML file.
   * @param expectedData The data we expect to get back from the score function.
   * @throws IOException
   */
  private void runRTest(
      SpecificRecord predictor,
      String pmmlFileName,
      String pmmlModelName,
      GenericRecord expectedData
  ) throws IOException {
    final Kiji kiji = getKiji();

    // Get the test table layout (we can use the same layout for all of the various R tests).
    final KijiTableLayout tableLayout =
        KijiTableLayouts.getTableLayout("generic-layout.json");

    // Populate the table with the data for this test.
    new InstanceBuilder(kiji)
        .withTable(TABLE_NAME, tableLayout)
            .withRow(ROW_KEY).withFamily("model").withQualifier("predictor").withValue(predictor)
        .build();

    final KijiTable table = kiji.openTable(TABLE_NAME);

    // Write model to a temporary file.
    final File pmmlModelFile = File.createTempFile(
        TestJpmmlScoreFunctionWithR.class.getName() + "-simple-linear-regression",
        ".xml"
    );

    final String pmmlModelString = IOUtils.toString(Resources.openSystemResource(pmmlFileName));
    pmmlModelFile.deleteOnExit();
    final PrintWriter pmmlPrintWriter = new PrintWriter(pmmlModelFile);
    try {
      pmmlPrintWriter.println(pmmlModelString);
    } finally {
      pmmlPrintWriter.flush();
      pmmlPrintWriter.close();
    }

    // Set up parameters for score function.
    final Map<String, String> parameters = JpmmlScoreFunction.parameters(
        // file
        "file://" + pmmlModelFile.getAbsolutePath(),
        // model name
        pmmlModelName,
        // result column
        new KijiColumnName("model", "predictor"),
        // result record name
        "MyResult"
    );

    // Create a KijiFreshnessManager and register some Fresheners.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener(
          TABLE_NAME,
          new KijiColumnName("model", "predicted"),
          new AlwaysFreshen(),
          new JpmmlScoreFunction(),
          parameters,
          false,
          false
      );
    } finally {
      manager.close();
    }

    // Open a new reader to pull in the new freshness policies. Allow 10 seconds so it is very
    // unlikely to timeout.
    final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
        .withTable(table)
        .withTimeout(10000)
        .build();
    try {
      final GenericRecord result = freshReader.get(
          table.getEntityId(ROW_KEY), KijiDataRequest.create("model", "predicted")
      ).getMostRecentValue("model", "predicted");
      assertNotNull(result);

      // Ensure score is correct.
      for (Schema.Field field : expectedData.getSchema().getFields()) {
        Object resultForField = result.get(field.name());
        Object expectedForField = expectedData.get(field.name());

        assert(null != resultForField): field;
        assert(null != expectedForField) : field;

        if (resultForField instanceof Double) {
          double actualDouble = ((Double) resultForField).doubleValue();
          double expectedDouble = ((Double) expectedForField).doubleValue();
          assertEquals(expectedDouble, actualDouble, 0.001);
        } else if (resultForField instanceof Utf8) {
          String actualString = resultForField.toString();
          String expectedString = expectedForField.toString();
          assertEquals(expectedString, actualString);
        } else {
          assert(false) : "Should not reach this code.";
        }
      }
    } finally {
      freshReader.close();
      table.release();
    }
  }
}
