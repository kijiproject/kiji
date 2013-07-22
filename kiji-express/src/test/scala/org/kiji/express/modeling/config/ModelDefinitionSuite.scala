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

package org.kiji.express.modeling.config

import scala.io.Source

import org.scalatest.FunSuite

import org.kiji.express.avro.AvroModelDefinition
import org.kiji.express.modeling.ExtractFn
import org.kiji.express.modeling.Extractor
import org.kiji.express.modeling.ScoreFn
import org.kiji.express.modeling.Scorer
import org.kiji.express.util.Resources.doAndClose
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.ToJson

class ModelDefinitionSuite extends FunSuite {
  val validDefinitionLocation: String =
      "src/test/resources/modelDefinitions/valid-model-definition.json"
  val invalidVersionDefinitionLocation: String =
      "src/test/resources/modelDefinitions/invalid-version-model-definition.json"
  val invalidNameDefinitionLocation: String =
      "src/test/resources/modelDefinitions/invalid-name-model-definition.json"
  val invalidExtractorDefinitionLocation: String =
      "src/test/resources/modelDefinitions/invalid-extractor-model-definition.json"
  val invalidScorerDefinitionLocation: String =
      "src/test/resources/modelDefinitions/invalid-scorer-model-definition.json"
  val invalidProtocolDefinitionLocation: String =
      "src/test/resources/modelDefinitions/invalid-protocol-model-definition.json"
  val invalidExtractorClassName: String =
      "src/test/resources/modelDefinitions/invalid-extractor-class-name.json"
  val invalidScorerClassName: String =
      "src/test/resources/modelDefinitions/invalid-scorer-class-name.json"

  test("A ModelDefinition can be created from specified parameters.") {
    val modelDefinition = ModelDefinition("name",
        "1.0.0",
        classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyExtractor],
        classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyScorer])
    assert("name" === modelDefinition.name)
    assert("1.0.0" === modelDefinition.version)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyExtractor] ===
        modelDefinition.extractorClass)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyScorer] ===
        modelDefinition.scorerClass)
  }

  test("Settings on a model definition can be modified.") {
    val modelDefinition = ModelDefinition("name",
      "1.0.0",
      classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyExtractor],
      classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyScorer])

    val modelDefinition2 = modelDefinition.withNewSettings(name = "name2")
    assert("name2" === modelDefinition2.name)
    assert("1.0.0" === modelDefinition2.version)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyExtractor] ===
        modelDefinition2.extractorClass)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyScorer] ===
        modelDefinition2.scorerClass)

    val modelDefinition3 = modelDefinition2.withNewSettings(version = "2.0.0")
    assert("name2" === modelDefinition3.name)
    assert("2.0.0" === modelDefinition3.version)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyExtractor] ===
        modelDefinition3.extractorClass)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyScorer] ===
        modelDefinition3.scorerClass)

    val modelDefinition4 = modelDefinition3.withNewSettings(
        extractor =
            classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.AnotherExtractor])
    assert("name2" === modelDefinition4.name)
    assert("2.0.0" === modelDefinition4.version)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.AnotherExtractor] ===
        modelDefinition4.extractorClass)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.MyScorer] ===
        modelDefinition4.scorerClass)

    val modelDefinition5 = modelDefinition4.withNewSettings(
      scorer = classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.AnotherScorer])
    assert("name2" === modelDefinition5.name)
    assert("2.0.0" === modelDefinition5.version)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.AnotherExtractor] ===
        modelDefinition5.extractorClass)
    assert(classOf[org.kiji.express.modeling.config.ModelDefinitionSuite.AnotherScorer] ===
        modelDefinition5.scorerClass)
  }

  test("ModelDefinition can be created from a path to a valid JSON file.") {
    val definition: ModelDefinition = ModelDefinition
        // TODO(EXP-53): Permit loading of configuration resources from a jar.
        .fromJsonFile("src/test/resources/modelDefinitions/valid-model-definition.json")

    assert("abandoned-cart-model" === definition.name)
    assert("1.0.0" === definition.version)
    assert(classOf[ModelDefinitionSuite.MyExtractor].getName === definition.extractorClass.getName)
    assert(classOf[ModelDefinitionSuite.MyScorer].getName === definition.scorerClass.getName)
  }

  test("ModelDefinition can write out JSON") {
    val originalJson: String = doAndClose(Source.fromFile(validDefinitionLocation)) { source =>
      source.mkString
    }
    val originalAvroObject: AvroModelDefinition = FromJson
        .fromJsonString(originalJson, AvroModelDefinition.SCHEMA$)
        .asInstanceOf[AvroModelDefinition]

    val definition: ModelDefinition = ModelDefinition.fromJson(originalJson)
    val newJson: String = definition.toJson()
    assert(ToJson.toAvroJsonString(originalAvroObject) === newJson)
  }

  test("ModelDefinition validates the name property") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJsonFile(invalidNameDefinitionLocation)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\nThe name of the model definition cannot be " +
        "the empty string." === thrown.getMessage)
  }

  test("ModelDefinition validates the version format") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJsonFile(invalidVersionDefinitionLocation)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\nModel definition version strings must match " +
        "the regex \"[0-9]+(.[0-9]+)*\" (1.0.0 would be valid)." === thrown.getMessage)
  }

  test("ModelDefinition validates the extractor class") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJsonFile(invalidExtractorDefinitionLocation)
    }
    val badExtractor = "org.kiji.express.modeling.config.ModelDefinitionSuite$MyBadExtractor"
    assert(thrown.getMessage.contains("An instance of the class \"%s\"".format(badExtractor) +
            " could not be cast as an instance of Extractor. Please ensure that you have" +
            " provided a valid class that inherits from the Extractor class."))
  }

  test("ModelDefinition validates the scorer class") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJsonFile(invalidScorerDefinitionLocation)
    }
    val badScorer = "org.kiji.express.modeling.config.ModelDefinitionSuite$MyBadScorer"
    assert(thrown.getMessage.contains("An instance of the class \"%s\"".format(badScorer) +
            " could not be cast as an instance of Scorer. Please ensure that you have" +
            " provided a valid class that inherits from the Scorer class."))
  }

  test("ModelDefinition validates the wire protocol") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJsonFile(invalidProtocolDefinitionLocation)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\n\"model_definition-0.1.0\" is the maximum " +
        "protocol version supported. The provided model definition is of protocol version: " +
        "\"model_definition-0.2.0\"" === thrown.getMessage)
  }

  test("ModelDefinition fails with a ValidationException given an invalid extractor class name") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJsonFile(invalidExtractorClassName)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }

  test("ModelDefinition fails with a ValidationException given an invalid scorer class name") {
    val thrown = intercept[ValidationException] {
      ModelDefinition.fromJsonFile(invalidScorerClassName)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }
}

object ModelDefinitionSuite {
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

  class MyBadExtractor { def howTrue: Boolean = true }

  class MyBadScorer { def howTrue: Boolean = true }
}
