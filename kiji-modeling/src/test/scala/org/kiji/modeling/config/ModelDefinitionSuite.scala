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

package org.kiji.modeling.config

import com.twitter.scalding.Source
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.express.util.Resources.resourceAsString
import org.kiji.modeling.DistanceFn
import org.kiji.modeling.Evaluator
import org.kiji.modeling.ExtractFn
import org.kiji.modeling.Extractor
import org.kiji.modeling.Preparer
import org.kiji.modeling.ScoreFn
import org.kiji.modeling.Scorer
import org.kiji.modeling.Trainer
import org.kiji.modeling.avro.AvroModelDefinition
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.ToJson

@RunWith(classOf[JUnitRunner])
class ModelDefinitionSuite extends FunSuite {
  val validDefinition: String = resourceAsString(
      "modelDefinitions/valid-model-definition.json")
  val invalidVersionDefinition: String = resourceAsString(
      "modelDefinitions/invalid-version-model-definition.json")
  val invalidNameDefinition: String = resourceAsString(
      "modelDefinitions/invalid-name-model-definition.json")
  val invalidPreparerDefinition: String = resourceAsString(
      "modelDefinitions/invalid-preparer-model-definition.json")
  val invalidTrainerDefinition: String = resourceAsString(
      "modelDefinitions/invalid-trainer-model-definition.json")
  val invalidExtractorDefinition: String = resourceAsString(
      "modelDefinitions/invalid-extractor-model-definition.json")
  val invalidScorerDefinition: String = resourceAsString(
      "modelDefinitions/invalid-scorer-model-definition.json")
  val invalidEvaluatorDefinition: String = resourceAsString(
    "modelDefinitions/invalid-evaluator-model-definition.json")
  val invalidProtocolDefinition: String = resourceAsString(
      "modelDefinitions/invalid-protocol-model-definition.json")
  val invalidPreparerClassNameDefinition: String = resourceAsString(
      "modelDefinitions/invalid-preparer-class-name.json")
  val invalidTrainerClassNameDefinition: String = resourceAsString(
      "modelDefinitions/invalid-trainer-class-name.json")
  val invalidExtractorClassNameDefinition: String = resourceAsString(
      "modelDefinitions/invalid-extractor-class-name.json")
  val invalidScorerClassNameDefinition: String = resourceAsString(
      "modelDefinitions/invalid-scorer-class-name.json")
  val invalidEvaluatorClassNameDefinition: String = resourceAsString(
    "modelDefinitions/invalid-evaluator-class-name.json")

  test("A ModelDefinition can be created from specified parameters.") {
    val modelDefinition = ModelDefinition(
        name = "name",
        version = "1.0.0",
        scoreExtractorClass = Some(classOf[ModelDefinitionSuite.MyExtractor]),
        preparerClass = Some(classOf[ModelDefinitionSuite.MyPreparer]),
        trainerClass = Some(classOf[ModelDefinitionSuite.MyTrainer]),
        scorerClass = Some(classOf[ModelDefinitionSuite.MyScorer]),
        evaluatorClass = Some(classOf[ModelDefinitionSuite.MyEvaluator]))

    // Validate the constructed definition.
    assert("name" === modelDefinition.name)
    assert("1.0.0" === modelDefinition.version)
    assert(classOf[ModelDefinitionSuite.MyPreparer] === modelDefinition.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.MyTrainer] === modelDefinition.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.MyScorer] === modelDefinition.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyExtractor] === modelDefinition.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition.evaluatorClass.get)

    // Serialize and deserialize the definition.
    val serialized = modelDefinition.toJson
    val deserialized = ModelDefinition.fromJson(serialized)

    // Validated the deserialized definition.
    assert("name" === deserialized.name)
    assert("1.0.0" === deserialized.version)
    assert(classOf[ModelDefinitionSuite.MyPreparer] === deserialized.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.MyTrainer] === deserialized.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.MyExtractor] === deserialized.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.MyScorer] === deserialized.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition.evaluatorClass.get)
  }

  test("Settings on a model definition can be modified.") {
    val modelDefinition = ModelDefinition(
        name = "name",
        version = "1.0.0",
        preparerClass = Some(classOf[ModelDefinitionSuite.MyPreparer]),
        trainerClass = Some(classOf[ModelDefinitionSuite.MyTrainer]),
        scoreExtractorClass = Some(classOf[ModelDefinitionSuite.MyExtractor]),
        scorerClass = Some(classOf[ModelDefinitionSuite.MyScorer]),
        evaluatorClass = Some(classOf[ModelDefinitionSuite.MyEvaluator]))


    val modelDefinition2 = modelDefinition.withNewSettings(name = "name2")
    assert("name2" === modelDefinition2.name)
    assert("1.0.0" === modelDefinition2.version)
    assert(classOf[ModelDefinitionSuite.MyPreparer] === modelDefinition2.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.MyTrainer] === modelDefinition2.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.MyExtractor] === modelDefinition2.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.MyScorer] === modelDefinition2.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition2.evaluatorClass.get)

    val modelDefinition3 = modelDefinition2.withNewSettings(version = "2.0.0")
    assert("name2" === modelDefinition3.name)
    assert("2.0.0" === modelDefinition3.version)
    assert(classOf[ModelDefinitionSuite.MyPreparer] === modelDefinition3.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.MyTrainer] === modelDefinition3.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.MyExtractor] === modelDefinition3.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.MyScorer] === modelDefinition3.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition3.evaluatorClass.get)

    val modelDefinition4 = modelDefinition3.withNewSettings(
        preparerClass = Some(classOf[ModelDefinitionSuite.AnotherPreparer]))
    assert("name2" === modelDefinition4.name)
    assert("2.0.0" === modelDefinition4.version)
    assert(classOf[ModelDefinitionSuite.AnotherPreparer] === modelDefinition4.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.MyTrainer] === modelDefinition4.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.MyExtractor] === modelDefinition4.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.MyScorer] === modelDefinition4.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition4.evaluatorClass.get)

    val modelDefinition5 = modelDefinition4.withNewSettings(
        trainerClass = Some(classOf[ModelDefinitionSuite.AnotherTrainer]))
    assert("name2" === modelDefinition5.name)
    assert("2.0.0" === modelDefinition5.version)
    assert(classOf[ModelDefinitionSuite.AnotherPreparer] === modelDefinition5.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherTrainer] === modelDefinition5.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.MyExtractor] === modelDefinition5.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.MyScorer] === modelDefinition5.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition5.evaluatorClass.get)

    val modelDefinition6 = modelDefinition5.withNewSettings(
        scoreExtractorClass = Some(classOf[ModelDefinitionSuite.AnotherExtractor]))
    assert("name2" === modelDefinition6.name)
    assert("2.0.0" === modelDefinition6.version)
    assert(classOf[ModelDefinitionSuite.AnotherPreparer] === modelDefinition6.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherTrainer] === modelDefinition6.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherExtractor]
        === modelDefinition6.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.MyScorer] === modelDefinition6.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition6.evaluatorClass.get)

    val modelDefinition7 = modelDefinition6.withNewSettings(
        scorerClass = Some(classOf[ModelDefinitionSuite.AnotherScorer]))
    assert("name2" === modelDefinition7.name)
    assert("2.0.0" === modelDefinition7.version)
    assert(classOf[ModelDefinitionSuite.AnotherPreparer] === modelDefinition7.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherTrainer] === modelDefinition7.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherExtractor]
        === modelDefinition7.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherScorer] === modelDefinition7.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.MyEvaluator] === modelDefinition7.evaluatorClass.get)

    val modelDefinition8 = modelDefinition7.withNewSettings(
      evaluatorClass = Some(classOf[ModelDefinitionSuite.AnotherEvaluator]))
    assert("name2" === modelDefinition8.name)
    assert("2.0.0" === modelDefinition8.version)
    assert(classOf[ModelDefinitionSuite.AnotherPreparer] === modelDefinition8.preparerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherTrainer] === modelDefinition8.trainerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherExtractor]
      === modelDefinition8.scoreExtractorClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherScorer] === modelDefinition8.scorerClass.get)
    assert(classOf[ModelDefinitionSuite.AnotherEvaluator] === modelDefinition8.evaluatorClass.get)
  }

  test("ModelDefinition can be created from a path to a valid JSON file.") {
    val definition: ModelDefinition = ModelDefinition
        .fromJsonFile("src/test/resources/modelDefinitions/valid-model-definition.json")

    assert("name" === definition.name)
    assert("1.0.0" === definition.version)
    assert(classOf[ModelDefinitionSuite.MyPreparer].getName
        === definition.preparerClass.get.getName)
    assert(classOf[ModelDefinitionSuite.MyExtractor].getName ===
        definition.scoreExtractorClass.get.getName)
    assert(classOf[ModelDefinitionSuite.MyScorer].getName === definition.scorerClass.get.getName)
    assert(classOf[ModelDefinitionSuite.MyEvaluator].getName ===
        definition.evaluatorClass.get.getName)
  }

  test("ModelDefinition can write out JSON") {
    val originalAvroObject: AvroModelDefinition = FromJson
        .fromJsonString(validDefinition, AvroModelDefinition.SCHEMA$)
        .asInstanceOf[AvroModelDefinition]

    val definition: ModelDefinition = ModelDefinition.fromJson(validDefinition)
    val newJson: String = definition.toJson
    assert(ToJson.toAvroJsonString(originalAvroObject) === newJson)
  }

  test("ModelDefinition validates the name property") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJson(invalidNameDefinition)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\nThe name of the model definition cannot be " +
        "the empty string." === thrown.getMessage)
  }

  test("ModelDefinition validates the version format") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJson(invalidVersionDefinition)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\nModel definition version strings must match " +
        "the regex \"[0-9]+(.[0-9]+)*\" (1.0.0 would be valid)." === thrown.getMessage)
  }

  test("ModelDefinition validates the preparer class") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidPreparerDefinition)
    }
    val badPreparer = "org.kiji.modeling.config.ModelDefinitionSuite$MyBadPreparer"
    assert(thrown.getMessage.contains("An instance of the class \"%s\"".format(badPreparer) +
        " could not be cast as an instance of Preparer. Please ensure that you have" +
        " provided a valid class that inherits from the Preparer class."))
  }

  test("ModelDefinition validates the trainer class") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidTrainerDefinition)
    }
    val badTrainer = "org.kiji.modeling.config.ModelDefinitionSuite$MyBadTrainer"
    assert(thrown.getMessage.contains("An instance of the class \"%s\"".format(badTrainer) +
        " could not be cast as an instance of Trainer. Please ensure that you have" +
        " provided a valid class that inherits from the Trainer class."))
  }

  test("ModelDefinition validates the extractor class") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidExtractorDefinition)
    }
    val badExtractor = "org.kiji.modeling.config.ModelDefinitionSuite$MyBadExtractor"
    assert(thrown.getMessage.contains("An instance of the class \"%s\"".format(badExtractor) +
            " could not be cast as an instance of Extractor. Please ensure that you have" +
            " provided a valid class that inherits from the Extractor class."))
  }

  test("ModelDefinition validates the scorer class") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidScorerDefinition)
    }
    val badScorer = "org.kiji.modeling.config.ModelDefinitionSuite$MyBadScorer"
    assert(thrown.getMessage.contains("An instance of the class \"%s\"".format(badScorer) +
            " could not be cast as an instance of Scorer. Please ensure that you have" +
            " provided a valid class that inherits from the Scorer class."))
  }

  test("ModelDefinition validates the evaluator class") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidEvaluatorDefinition)
    }
    val badEvaluator = "org.kiji.modeling.config.ModelDefinitionSuite$MyBadEvaluator"
    assert(thrown.getMessage.contains("An instance of the class \"%s\"".format(badEvaluator) +
      " could not be cast as an instance of Evaluator. Please ensure that you have" +
      " provided a valid class that inherits from the Evaluator class."))
  }

  test("ModelDefinition validates the wire protocol") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJson(invalidProtocolDefinition)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\n\"model_definition-0.3.0\" is the maximum " +
        "protocol version supported. The provided model definition is of protocol version: " +
        "\"model_definition-7.3.0\"" === thrown.getMessage)
  }

  test("ModelDefinition fails with a ValidationException given an invalid preparer class name") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidPreparerClassNameDefinition)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }

  test("ModelDefinition fails with a ValidationException given an invalid trainer class name") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidTrainerClassNameDefinition)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }

  test("ModelDefinition fails with a ValidationException given an invalid extractor class name") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidExtractorClassNameDefinition)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }

  test("ModelDefinition fails with a ValidationException given an invalid scorer class name") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidScorerClassNameDefinition)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }

  test("ModelDefinition fails with a ValidationException given an invalid evaluator class name") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJson(invalidEvaluatorClassNameDefinition)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
      "class name and that it is available on your classpath." === thrown.getMessage)
  }

  test("ModelDefinition validates empty phase set") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition(
        name = "name",
        version = "1.0.0")
    }
    assert(thrown.getMessage.contains("The model defines no phases."))
  }

  test("ModelDefinition validates missing train phase") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition(
          name = "name",
          version = "1.0.0",
          preparerClass = Some(classOf[ModelDefinitionSuite.MyPreparer]),
          scoreExtractorClass = Some(classOf[ModelDefinitionSuite.MyExtractor]),
          scorerClass = Some(classOf[ModelDefinitionSuite.MyScorer]),
          evaluatorClass = Some(classOf[ModelDefinitionSuite.MyEvaluator]))
    }
    assert(thrown.getMessage.contains("Unsupported combination of phases."))
  }

  test("ModelDefinition validates missing score phase") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition(
        name = "name",
        version = "1.0.0",
        preparerClass = Some(classOf[ModelDefinitionSuite.MyPreparer]),
        trainerClass = Some(classOf[ModelDefinitionSuite.MyTrainer]),
        evaluatorClass = Some(classOf[ModelDefinitionSuite.MyEvaluator]))
    }
    assert(thrown.getMessage.contains("Unsupported combination of phases."))
  }
}

object ModelDefinitionSuite {
  class MyPreparer extends Preparer {
    override def prepare(input: Map[String, Source], output: Map[String, Source]):
        Boolean = { true }
  }

  class AnotherPreparer extends MyPreparer

  class MyTrainer extends Trainer {
    override def train(input: Map[String, Source], output: Map[String, Source]): Boolean = { true }
  }

  class AnotherTrainer extends MyTrainer

  class MyExtractor extends Extractor {
    def extractFn: ExtractFn[_, _] = extract('textLine -> 'first) { line: String =>
      line
          .split("""\s+""")
          .head
    }
  }

  class AnotherExtractor extends MyExtractor

  class MyScorer extends Scorer {
    def scoreFn: ScoreFn[_, _] = score('first) { line: String =>
      line
          .split("""\s+""")
          .size
    }
  }

  class AnotherScorer extends MyScorer

  class MyEvaluator extends Evaluator {
    def distanceFn: DistanceFn[_, _] = DistanceFn(('c1, 'c2) -> 'count) {
      columns: (Double, Double) => {
        scala.math.abs(columns._1 - columns._2)
      }
    }
  }

  class AnotherEvaluator extends MyEvaluator

  class MyBadPreparer { def howTrue: Boolean = true }

  class MyBadTrainer { def howTrue: Boolean = true }

  class MyBadExtractor { def howTrue: Boolean = true }

  class MyBadScorer { def howTrue: Boolean = true }

  class MyBadEvaluator { def howTrue: Boolean = true }
}
