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

package org.kiji.mapreduce.lib.produce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiRowData;


/**
 * A producer which uses a full regular expression match on a string
 * input column to generate a new column.  The regular expression
 * should have a single group (things inside parentheses), which will
 * be extracted and used as the new data.
 */
public abstract class RegexProducer extends SingleInputProducer {
  private static final Logger LOG = LoggerFactory.getLogger(RegexProducer.class);

  private Pattern mPattern;

  /**
   * @return the regular expression string (will match regex against full string).
   */
  protected abstract String getRegex();

  /** {@inheritDoc} */
  @Override
  public void setup(KijiContext context) throws IOException {
    super.setup(context);
    mPattern = Pattern.compile(getRegex());
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    if (!input.containsColumn(getInputColumnName().getFamily(),
            getInputColumnName().getQualifier())) {
      LOG.debug("No " + getInputColumnName().getName() + " for entity: " + input.getEntityId());
    }
    String string = input.getMostRecentValue(getInputColumnName().getFamily(),
        getInputColumnName().getQualifier()).toString();

    // Run the regex on the input string.
    Matcher matcher = mPattern.matcher(string);
    if (matcher.matches()) {
      if (matcher.groupCount() == 1) {
        context.put(matcher.group(1));
      }
    } else {
      LOG.debug(input.getEntityId().toString() + "'s data '" + string + "' does not match "
          + mPattern.pattern());
    }
  }
}
