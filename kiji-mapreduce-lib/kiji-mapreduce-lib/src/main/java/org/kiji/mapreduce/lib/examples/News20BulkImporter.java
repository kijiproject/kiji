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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;

/**
 * <p>A bulk importer that takes qualified_path/raw_article key/value pairs,
 * and loads these into kiji.  The article name is specified by the
 * parent folder and name of this article (This is to guarantee unique names).
 * The classification is specified by the parent
 * folder to this article.  The raw article to store is passed in as the value.</p>
 *
 * <p>For example, the calling the produce() method with key:value of<p>
 * <code>"some/path/sci.med/12345":"This is the article text"</code>
 * <p>will generate a single row in kiji with fields:<p>
 * <ul>
 *   <li>name: "sci.med.12345"</li>
 *   <li>category: "sci.med"</li>
 *   <li>raw_article: "This is the article text"</li>
 * </ul>
 */
public class News20BulkImporter extends KijiBulkImporter<Text, Text> {
  /** The family to write input data to. */
  public static final String FAMILY = "info";

  /** Qualifier storing the article name. */
  public static final String ARTICLE_NAME_QUALIFIER = "name";
  /** Qualifier storing the article category. */
  public static final String CATEGORY_QUALIFIER = "category";
  /** Qualifier storing the raw text of an article. */
  public static final String RAW_ARTICLE_QUALIFIER = "raw_article";

  /**
   * Reads a single news article, and writes its contents to a new kiji row,
   * indexed by the article's name (A string consisting of the parent folder, and
   * this article's hash), and the a priori categorization of this article.
   *
   * @param key The fully qualified path to the current file we're reading.
   * @param value The raw data to insert into this column.
   * @param context The context to write to.
   * @throws IOException if there is an error.
   */
  @Override
  public void produce(Text key, Text value, KijiTableContext context)
      throws IOException {
    Path qualifiedPath = new Path(key.toString());

    // Category is specified on the containing folder.
    String category = qualifiedPath.getParent().getName();
    // Name is the concatenation of category and file name.
    String name = category + "." + qualifiedPath.getName();

    // write name, category, and raw article.
    EntityId entity = context.getEntityId(name);
    context.put(entity, FAMILY, ARTICLE_NAME_QUALIFIER, name);
    context.put(entity, FAMILY, CATEGORY_QUALIFIER, category);
    context.put(entity, FAMILY, RAW_ARTICLE_QUALIFIER, value.toString());
  }
}
