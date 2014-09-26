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
package org.kiji.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

public class TestStreamRedirect {

  private String simpleTest(
      final String expected
  ) {
    final InputStream in = new ByteArrayInputStream(expected.getBytes());
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final StreamRedirect redirect = StreamRedirect.create(in, out);
    redirect.run();

    return out.toString();
  }

  @Test
  public void simpleRedirects() {
    Assert.assertEquals(
        "abcde",
        simpleTest("abcde")
    );
    Assert.assertEquals(
        "abcde\nfghij",
        simpleTest("abcde\nfghij")
    );
    Assert.assertEquals(
        "\nabcde",
        simpleTest("\nabcde")
    );
    // The trailing newline is not persisted.
    Assert.assertEquals(
        "abcde",
        simpleTest("abcde\n")
    );
  }
}
