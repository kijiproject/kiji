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

package org.kiji.express.modeling

import scala.io.Source

import org.scalatest.FunSuite

import org.kiji.express.Resources.doAndClose
import org.kiji.express.avro.AvroModelDefinition
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
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJsonFile(invalidExtractorDefinitionLocation)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\nThe class " +
        "\"org.kiji.express.modeling.ModelDefinitionSuite$MyBadExtractor\" does not implement " +
        "the Extractor trait." === thrown.getMessage)
  }

  test("ModelDefinition validates the scorer class") {
    val thrown = intercept[ModelDefinitionValidationException] {
      ModelDefinition.fromJsonFile(invalidScorerDefinitionLocation)
    }
    assert(ModelDefinition.VALIDATION_MESSAGE + "\nThe class " +
        "\"org.kiji.express.modeling.ModelDefinitionSuite$MyBadScorer\" does not implement the " +
        "Scorer trait." === thrown.getMessage)
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

  class MyScorer extends Scorer {
    def scoreFn: ScoreFn[_, _] = score('first) { line: String =>
      line
          .split("""\s+""")
          .size
    }
  }

  class MyBadExtractor { def howTrue: Boolean = true }

  class MyBadScorer { def howTrue: Boolean = true }
}
