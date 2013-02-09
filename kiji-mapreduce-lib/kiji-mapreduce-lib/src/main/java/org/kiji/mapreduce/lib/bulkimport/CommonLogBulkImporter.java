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

package org.kiji.mapreduce.lib.bulkimport;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.lib.util.CommonLogParser;
import org.kiji.mapreduce.lib.util.CommonLogParser.Field;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;

/**
 * Bulk importer that handles the NCSA Common Log Format.
 *
 * Details about this format here:
 * {@linkurl http://www.w3.org/Daemon/User/Config/Logging.html#common-logfile-format}
 *
 * {@inheritDoc}
 */
@ApiAudience.Public
public final class CommonLogBulkImporter extends DescribedInputTextBulkImporter {
  private static final Logger LOG =
      LoggerFactory.getLogger(CommonLogBulkImporter.class);

  /** {@inheritDoc} */
  @Override
  public void produce(Text value, KijiTableContext context) throws IOException {
    Map<Field, String> fieldMap;
    try {
      fieldMap = CommonLogParser.get().parseCommonLog(value.toString());
    } catch (ParseException pe) {
      reject(value, context, "Unable to parse row: " + value.toString());
      return;
    }

    Field entityIdSource = Field.valueOf(getEntityIdSource());
    EntityId eid = context.getEntityId(fieldMap.get(entityIdSource));

    for (KijiColumnName kijiColumnName : getDestinationColumns()) {
      Field source = Field.valueOf(getSource(kijiColumnName));
      String fieldValue = fieldMap.get(source);
      if (fieldValue != null) {
        context.put(eid,  kijiColumnName.getFamily(), kijiColumnName.getQualifier(), fieldValue);
      } else {
        reject(value, context, "Log file missing field: " + source);
      }
    }
  }
}
