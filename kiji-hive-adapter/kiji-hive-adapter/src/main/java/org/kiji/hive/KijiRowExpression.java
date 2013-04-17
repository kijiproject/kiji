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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.utils.AvroTypeAdapter;
import org.kiji.hive.utils.HiveTypes.*;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

/**
 * A KijiRowExpression is a string that addresses a piece of data inside a KijiTable row.
 *
 * <p>Data can be addressed by specifying an entire family, a specific
 * column, or even a particular field within an cell. This may someday
 * be extended to allow wildcards, for example to create arrays of
 * fields inside records across multiple cells.</p>
 *
 * <p>Valid expressions:</p>
 * <li> family - map[string, array[struct[int, fields...]]]
 * <li> family[0] - map[string, struct[int, fields...]]
 * <li> family:qualifier - array[struct[int, fields...]]
 * <li> family:qualifier[3] - struct[int, fields...]
 * <li> family:qualifier[0].field - fieldtype
 * <li> family:qualifier[-1].timestamp - timestamp (of the oldest cell)
 */
public class KijiRowExpression {
  private static final Logger LOG = LoggerFactory.getLogger(KijiRowExpression.class);

  /** The parsed expression. */
  private final Expression mExpression;

  /**
   * Creates an expression.
   *
   * @param expression The expression string.
   * @param typeInfo The Hive type the expression is mapped to.
   */
  public KijiRowExpression(String expression, TypeInfo typeInfo) {
    mExpression = new Parser().parse(StringUtils.trim(expression), typeInfo);
  }

  /**
   * Evaluates an expression in the context of a Kiji row.
   *
   * @param row A kiji row.
   * @return The data addressed by the expression.
   * @throws IOException If there is an IO error.
   */
  public Object evaluate(KijiRowData row) throws IOException {
    return new Evaluator().evaluate(mExpression, row);
  }

  /**
   * Gets the data request required to evaluate this expression.
   *
   * @return The data request.
   */
  public KijiDataRequest getDataRequest() {
    return mExpression.getDataRequest();
  }

  /**
   * A parsed expression.
   */
  private interface Expression {
    /**
     * Determies whether the expression represents a value (not an operator).
     *
     * @return Whether this expression is a value.
     */
    boolean isValue();

    /**
     * Gets the value in the context of a row.
     *
     * @param row A kiji row.
     * @return The value.
     * @throws IOException If there is an IO error.
     */
    Object getValue(KijiRowData row) throws IOException;

    /**
     * Gets the operands of this operator.
     *
     * @return The operands.
     * @throws UnsupportedOperationException If this is not an operator.
     */
    List<Expression> getOperands();

    /**
     * Evaluates the operator expression given the operands.
     *
     * @param operandValues The values of the operands.
     * @return The result.
     */
    Object eval(List<Object> operandValues);

    /**
     * Gets the data request required to evaluate this expression.
     *
     * @return The data request.
     */
    KijiDataRequest getDataRequest();
  }

  /**
   * An expression that represents a data value on its own (not an operator).
   */
  private abstract static class ValueExpression implements Expression {
    /** The Hive type of the value. */
    private final TypeInfo mTypeInfo;

    /** The KijiColumnName. */
    private final KijiColumnName mKijiColumnName;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param kijiColumnName The Kiji column for this expression.
     */
    protected ValueExpression(TypeInfo typeInfo, KijiColumnName kijiColumnName) {
      mTypeInfo = typeInfo;
      mKijiColumnName = kijiColumnName;
    }

    /**
     * Gets the Hive type.
     *
     * @return The Hive type.
     */
    protected TypeInfo getTypeInfo() {
      return mTypeInfo;
    }

    /**
     * Gets an Avro type adapter for converting Avro and Hive data/types.
     *
     * @return The adapter.
     */
    protected AvroTypeAdapter getAvroTypeAdapter() {
      return AvroTypeAdapter.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isValue() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public List<Expression> getOperands() {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Object eval(List<Object> operandValues) {
      throw new UnsupportedOperationException();
    }

    /**
     * Gets the Kiji column family name.
     *
     * @return The Kiji column family name.
     */
    protected String getFamily() {
      return mKijiColumnName.getFamily();
    }

    /**
     * Gets the Kiji column qualifier name.
     *
     * @return The Kiji column qualifier name.
     */
    protected String getQualifier() {
      return mKijiColumnName.getQualifier();
    }
  }

  /**
   * An expression that reads a single cell value from a Kiji table column.
   */
  private static class FlatValueExpression extends ValueExpression {
    /** The index of the cell to read from the Kiji table column (newest is zero). */
    private final int mIndex;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param kijiColumnName The Kiji Column name
     * @param index The index of the cell to read from the column (newest is zero).
     */
    public FlatValueExpression(
        TypeInfo typeInfo, KijiColumnName kijiColumnName, int index) {
      super(typeInfo, kijiColumnName);
      if (index < -1) {
        throw new IllegalArgumentException(
            "Illegal index [" + index + "] for column expression "
            + kijiColumnName.toString());
      }
      mIndex = index;
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(KijiRowData row) throws IOException {
      if (!row.containsColumn(getFamily(), getQualifier())) {
        // TODO Consider logging LOG.warn("Nothing found for {}:{}", getFamily(), getQualifier());
        return null;
      }

      final TypeInfo cellTypeInfo = getCellTypeInfo();

      final NavigableMap<Long, Object> cellMap =
          row.getValues(getFamily(), getQualifier());

      Map.Entry<Long, Object> cell;
      if (-1 == mIndex) {
        cell = cellMap.lastEntry();
      } else {
        final Iterator<Map.Entry<Long, Object>> cellIterator = cellMap.entrySet().iterator();
        for (int i = 0; i < mIndex && cellIterator.hasNext(); i++) {
          cellIterator.next();
        }
        if (!cellIterator.hasNext()) {
          return null;
        }
        cell = cellIterator.next();
      }

      final HiveStruct struct = new HiveStruct();
      // Add the cell timestamp.
      struct.add(new Timestamp(cell.getKey().longValue()));
      // Add the cell value.
      struct.add(getAvroTypeAdapter().toHiveType(cellTypeInfo, cell.getValue()));
      return struct;
    }

    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      // Indexes start from 0, whereas maxVersions starts from 1 so we need to adjust for this
      int maxVersions = mIndex + 1;
      if (mIndex == -1) {
        // If we are getting the oldest cell, we need all versions.
        maxVersions = HConstants.ALL_VERSIONS;
      }

      KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(maxVersions).add(getFamily(), getQualifier());
      return builder.build();
    }

    /**
     * Gets the declared (therefore, the target) Hive type for the Kiji cell data.
     *
     * @return The Hive type.
     */
    private TypeInfo getCellTypeInfo() {
      // TODO: Move this work to the constructor so it is only computed once.
      final StructTypeInfo structTypeInfo = (StructTypeInfo) getTypeInfo();
      return structTypeInfo.getAllStructFieldTypeInfos().get(1);
    }
  }

  /**
   * An expression that reads all the cells from a Kiji table column.
   */
  private static class AllValuesExpression extends ValueExpression {
    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param kijiColumnName  The Kiji column name.
     */
    public AllValuesExpression(TypeInfo typeInfo, KijiColumnName kijiColumnName) {
      super(typeInfo, kijiColumnName);
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(KijiRowData row) throws IOException {
      final HiveList<HiveStruct> result = new HiveList<HiveStruct>();
      if (!row.containsColumn(getFamily(), getQualifier())) {
        return result;
      }

      final TypeInfo cellTypeInfo = getCellTypeInfo();
      final Schema schema = getAvroTypeAdapter().toAvroSchema(cellTypeInfo);

      final NavigableMap<Long, Object> cellMap =
          row.getValues(getFamily(), getQualifier());
      for (Map.Entry<Long, Object> cell : cellMap.entrySet()) {
        final HiveStruct struct = new HiveStruct();
        // Add the cell timestamp.
        struct.add(new Timestamp(cell.getKey().longValue()));
        // Add the cell value.
        struct.add(getAvroTypeAdapter().toHiveType(cellTypeInfo, cell.getValue()));
        result.add(struct);
      }

      return result;
    }

    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef()
          .withMaxVersions(HConstants.ALL_VERSIONS)
          .add(getFamily(), getQualifier());
      return builder.build();
    }

    /**
     * Gets the declared (therefore, the target) Hive type for the Kiji cell value.
     *
     * @return The Hive type.
     */
    private TypeInfo getCellTypeInfo() {
      // TODO: Move this work to the constructor so it is only computed once.
      final ListTypeInfo listTypeInfo = (ListTypeInfo) getTypeInfo();
      final StructTypeInfo structTypeInfo = (StructTypeInfo) listTypeInfo.getListElementTypeInfo();
      return structTypeInfo.getAllStructFieldTypeInfos().get(1);
    }
  }

  /**
   * Turns a string expression into a structured tree that can be
   * evaluated given the data in a Kiji row.
   */
  private static class Parser {
    /** The regular expression for Kiji row expressions. */
    private static final String REGEX =
        "([A-Za-z0-9_]*)(:([^.\\[]*))?(\\[(-?\\d+)\\])?([.]([a-z]+))*";

    /** The compiled pattern for Kiji row expressions. */
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    /**
     * Parses a string expression.
     *
     * @param expression The expression string.
     * @param typeInfo The target Hive type the evaluated expression should be in.
     * @return The parsed expression.
     */
    public Expression parse(String expression, TypeInfo typeInfo) {
      // TODO: Use ANTLR or some other engine to allow for more
      // expressive expressions in the future.

      final Matcher matcher = PATTERN.matcher(expression);
      if (!matcher.matches()) {
        // TODO: Make a new type for this exception.
        throw new RuntimeException("Invalid kiji row expression: " + expression);
      }

      // TODO: Support reading the row key.
      final String family = matcher.group(1);
      final String qualifier = matcher.group(3);
      KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
      if (null == matcher.group(4)) {
        return new AllValuesExpression(typeInfo, kijiColumnName);
      }
      Integer index = Integer.valueOf(matcher.group(5));
      if (index < -1) {
        throw new RuntimeException("Invalid index(must be >= -1): " + index);
      }
      return new FlatValueExpression(typeInfo, kijiColumnName, index);

      // TODO: Parse other operators on the values.
    }
  }

  /**
   * Evaluates a parsed Kiji row expression.
   */
  private static class Evaluator {
    /**
     * Evaluates a parsed expression in the context of a Kiji row.
     *
     * @param expression A parsed expression.
     * @param row A Kiji table row.
     * @return The evaluated expression data.
     * @throws IOException If there is an IO error reading from the Kiji row.
     */
    public Object evaluate(Expression expression, KijiRowData row) throws IOException {
      if (expression.isValue()) {
        return expression.getValue(row);
      }
      List<Object> operandValues = new ArrayList<Object>();
      for (Expression operand : expression.getOperands()) {
        operandValues.add(evaluate(operand, row));
      }
      return expression.eval(operandValues);
    }
  }
}
