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

package org.kiji.modeling

import com.twitter.scalding.Source
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

class TestPreparer extends Preparer {
  class WordCountJob(input: Map[String, Source], output: Map[String, Source]) extends PreparerJob {
    input("input")
      .flatMap('line -> 'word) { line : String => line.split("\\s+") }
      .groupBy('word) { _.size }
      .write(output)
  }

  override def prepare(input: Map[String, Source], output: Map[String, Source]): Boolean = {
    new WordCountJob(input, output).run
    true
  }
}

@RunWith(classOf[JUnitRunner])
class PreparerSuite extends FunSuite {
  test("A preparerClass can be constructed via reflection") {
    classOf[TestPreparer].newInstance
  }
}
