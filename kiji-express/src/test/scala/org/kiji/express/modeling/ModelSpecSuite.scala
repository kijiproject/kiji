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
import org.kiji.express.avro.AvroModelSpec
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.ToJson

class ModelSpecSuite extends FunSuite {
  val validSpecLocation: String = "src/test/resources/modelSpecs/valid-model-spec.json"
  val invalidVersionSpecLocation: String =
      "src/test/resources/modelSpecs/invalid-version-model-spec.json"
  val invalidNameSpecLocation: String =
      "src/test/resources/modelSpecs/invalid-name-model-spec.json"
  val invalidExtractorSpecLocation: String =
      "src/test/resources/modelSpecs/invalid-extractor-model-spec.json"
  val invalidScorerSpecLocation: String =
      "src/test/resources/modelSpecs/invalid-scorer-model-spec.json"
  val invalidProtocolSpecLocation: String =
      "src/test/resources/modelSpecs/invalid-protocol-model-spec.json"
  val invalidExtractorClassName: String =
      "src/test/resources/modelSpecs/invalid-extractor-class-name.json"
  val invalidScorerClassName: String =
      "src/test/resources/modelSpecs/invalid-scorer-class-name.json"

  test("ModelSpec can be created from a path to a valid JSON file.") {
    val spec: ModelSpec = ModelSpec
        // TODO(EXP-53): Permit loading of configuration resources from a jar.
        .fromJsonFile("src/test/resources/modelSpecs/valid-model-spec.json")

    assert("abandoned-cart-model" === spec.name)
    assert("1.0.0" === spec.version)
    assert(classOf[ModelSpecSuite.MyExtractor].getName === spec.extractorClass.getName)
    assert(classOf[ModelSpecSuite.MyScorer].getName === spec.scorerClass.getName)
  }

  test("ModelSpec can write out JSON") {
    val originalJson: String = doAndClose(Source.fromFile(validSpecLocation)) { source =>
      source.mkString
    }
    val originalAvroObject: AvroModelSpec = FromJson
        .fromJsonString(originalJson, AvroModelSpec.SCHEMA$)
        .asInstanceOf[AvroModelSpec]

    val spec: ModelSpec = ModelSpec.fromJson(originalJson)
    val newJson: String = spec.toJson()
    assert(ToJson.toAvroJsonString(originalAvroObject) === newJson)
  }

  test("ModelSpec validates the name property") {
    val thrown = intercept[ModelSpecValidationException] {
      ModelSpec.fromJsonFile(invalidNameSpecLocation)
    }
    assert(ModelSpec.VALIDATION_MESSAGE + "\nThe name of the model spec cannot be the empty string."
        === thrown.getMessage)
  }

  test("ModelSpec validates the version format") {
    val thrown = intercept[ModelSpecValidationException] {
      ModelSpec.fromJsonFile(invalidVersionSpecLocation)
    }
    assert(ModelSpec.VALIDATION_MESSAGE + "\nModel spec version strings must match the regex " +
        "\"[0-9]+(.[0-9]+)*\" (1.0.0 would be valid)." === thrown.getMessage)
  }

  test("ModelSpec validates the extractor class") {
    val thrown = intercept[ModelSpecValidationException] {
      ModelSpec.fromJsonFile(invalidExtractorSpecLocation)
    }
    assert(ModelSpec.VALIDATION_MESSAGE + "\nThe class " +
        "\"org.kiji.express.modeling.ModelSpecSuite$MyBadExtractor\" does not implement the " +
        "Extractor trait." === thrown.getMessage)
  }

  test("ModelSpec validates the scorer class") {
    val thrown = intercept[ModelSpecValidationException] {
      ModelSpec.fromJsonFile(invalidScorerSpecLocation)
    }
    assert(ModelSpec.VALIDATION_MESSAGE + "\nThe class " +
        "\"org.kiji.express.modeling.ModelSpecSuite$MyBadScorer\" does not implement the Scorer " +
        "trait." === thrown.getMessage)
  }

  test("ModelSpec validates the wire protocol") {
    val thrown = intercept[ModelSpecValidationException] {
      ModelSpec.fromJsonFile(invalidProtocolSpecLocation)
    }
    assert(ModelSpec.VALIDATION_MESSAGE + "\n\"model_spec-0.1.0\" is the maximum protocol " +
        "version supported. The provided model spec is of protocol version: \"model_spec-0.2.0\""
        === thrown.getMessage)
  }

  test("ModelSpec will fail with a ValidationException if given an invalid extractor class name") {
    val thrown = intercept[ValidationException] {
      ModelSpec.fromJsonFile(invalidExtractorClassName)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }

  test("ModelSpec will fail with a ValidationException if given an invalid scorer class name") {
    val thrown = intercept[ValidationException] {
      ModelSpec.fromJsonFile(invalidScorerClassName)
    }
    assert("The class \"blah\" could not be found. Please ensure that you have provided a valid " +
        "class name and that it is available on your classpath." === thrown.getMessage)
  }
}

object ModelSpecSuite {
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
