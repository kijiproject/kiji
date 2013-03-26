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

package org.kiji.schema.shell.ddl

import org.specs2.mutable._

class TestColumnName extends SpecificationWithJUnit {
  "ColumnName" should {
    "support equals" in {
      val col1 = new ColumnName("a", "b")
      val col2 = new ColumnName("a", "b")

      col1.equals(col2) mustEqual true
      col2.equals(col1) mustEqual true
      col1.hashCode() mustEqual col2.hashCode()
    }

    "support not equal to a nonColumnName" in {
      val col1 = new ColumnName("a", "b")
      col1.equals(6) mustEqual false
    }

    "support not equal to a different family" in {
      val col1 = new ColumnName("a", "b")
      val col2 = new ColumnName("c", "b")
      col1.equals(col2) mustEqual false
      col2.equals(col1) mustEqual false
    }

    "support not equal to a different qualifier" in {
      val col1 = new ColumnName("a", "b")
      val col2 = new ColumnName("a", "d")
      col1.equals(col2) mustEqual false
      col2.equals(col1) mustEqual false
    }
  }
}
