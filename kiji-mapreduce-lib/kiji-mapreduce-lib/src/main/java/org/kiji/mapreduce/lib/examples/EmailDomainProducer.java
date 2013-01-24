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

package org.kiji.mapreduce.lib.examples;

import java.io.IOException;

import org.kiji.mapreduce.KijiProducer;
import org.kiji.mapreduce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Extracts the domain of a user by reading their email address from the <i>info:email</i> column.
 *
 * <p>The extracted email domain is written to the <i>derived:domain</i> column.</p>
 *
 * <p>To run this from the command line:</p>
 *
 * <pre>
 * $ $KIJI_HOME/bin/kiji produce \
 * &gt;   --input=kiji:tablename \
 * &gt;   --producer=org.kiji.mapreduce.lib.examples.EmailDomainProducer
 * &gt;   --output=kiji \
 * </pre>
 */
public class EmailDomainProducer extends KijiProducer {
  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // We only need to read the most recent email address field from the user's row.
    return new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "email")
            .withMaxVersions(1));
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return "derived:domain";
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    if (!input.containsColumn("info", "email")) {
      // This user doesn't have an email address.
      return;
    }
    String email = input.getMostRecentValue("info", "email").toString();
    int atSymbol = email.indexOf("@");
    if (atSymbol < 0) {
      // Couldn't find the '@' in the email address. Give up.
      return;
    }
    String domain = email.substring(atSymbol + 1);
    context.put(domain);
  }
}
