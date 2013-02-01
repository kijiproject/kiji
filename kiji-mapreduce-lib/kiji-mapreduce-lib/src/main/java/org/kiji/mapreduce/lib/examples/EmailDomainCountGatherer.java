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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiGatherer;
import org.kiji.mapreduce.MapReduceContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Computes the domain from the email, and outputs it as the key with an int count of 1.
 *
 * <p>When used with an <code>IntSumReducer</code>, the resulting dataset is a map from
 * email domains to the number of users with that email domain.</p>
 *
 * <p>To run this from the command line:<p>
 *
 * <pre>
 * $ $KIJI_HOME/bin/kiji gather \
 * &gt;   --input=kiji:tablename \
 * &gt;   --gatherer=org.kiji.mapreduce.lib.examples.EmailDomainCountGatherer \
 * &gt;   --reducer=org.kiji.mapreduce.lib.reduce.IntSumReducer \
 * &gt;   --output=text:email-domain-counts@1
 * </pre>
 */
public class EmailDomainCountGatherer extends KijiGatherer<Text, IntWritable> {
  /** A reusable output key instance to hold the email domain of the user. */
  private Text mDomain;

  /** A reusable output value instance to hold the integer count (always one). */
  private static final IntWritable ONE = new IntWritable(1);

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.create("info", "email");
  }

  /** {@inheritDoc} */
  @Override
  public void setup(MapReduceContext context) throws IOException {
    super.setup(context);
    mDomain = new Text();
  }

  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData input, MapReduceContext context)
      throws IOException {
    if (!input.containsColumn("info", "email")) {
      // No email data.
      return;
    }
    String email = input.getMostRecentValue("info", "email").toString();
    int atSymbol = email.indexOf("@");
    if (atSymbol < 0) {
      // Invalid email.
      return;
    }
    String domain = email.substring(atSymbol + 1);
    mDomain.set(domain);
    context.write(mDomain, ONE);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return IntWritable.class;
  }
}
