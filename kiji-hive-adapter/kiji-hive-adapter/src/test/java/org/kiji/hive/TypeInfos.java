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

package org.kiji.hive;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Hive TypeInfos used for tests.  All of these TypeInfos assume that the nested cells are strings.
 */
public final class TypeInfos {

  /** Utility class cannot be instantiated. */
  private TypeInfos() {}

  /** For RowExpressions of the form "family". */
  public static final TypeInfo FAMILY_MAP_ALL_VALUES =
      TypeInfoUtils.getTypeInfoFromTypeString(
          "map<string,array<struct<ts:timestamp,value:int>>>");

  /** For RowExpressions of the form "family[n]". */
  public static final TypeInfo FAMILY_MAP_FLAT_VALUE =
      TypeInfoUtils.getTypeInfoFromTypeString("map<string,struct<ts:timestamp,value:int>>");

  /** For RowExpressions of the form "family:qualifier". */
  public static final TypeInfo COLUMN_ALL_VALUES =
      TypeInfoUtils.getTypeInfoFromTypeString("array<struct<ts:timestamp,value:string>>");

  /** For RowExpressions of the form "family:qualifier[n]". */
  public static final TypeInfo COLUMN_FLAT_VALUE =
      TypeInfoUtils.getTypeInfoFromTypeString("struct<ts:timestamp,value:string>");

}
