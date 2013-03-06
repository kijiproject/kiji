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

package org.kiji.lang;

import java.io.Serializable;

import com.google.common.base.Objects;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.filter.KijiColumnFilter;

/**
 * Represents a column in a Kiji table.
 */
@SuppressWarnings("serial")
@ApiAudience.Public
@ApiStability.Unstable
public final class Column implements Serializable {
  /** Name of the column in the form: "family:qualifier". */
  private final String mName;
  /** Options relevant to input. */
  private final InputOptions mInputOptions;

  /**
   * Constructs a new column.
   *
   * @param name Name of the column in the form: "family:qualifier".
   * @param inputOptions Options relevant to input.
   */
  public Column(String name, InputOptions inputOptions) {
    mName = name;
    mInputOptions = inputOptions;
  }

  /** @return Name of the column in the form: "family:qualifier". */
  public String name() {
    return mName;
  }

  /** @return Options relevant to input. */
  public InputOptions inputOptions() {
    return mInputOptions;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return mName;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Column)) {
      return false;
    }

    final Column column = (Column) other;
    return Objects.equal(mName, column.mName)
        && Objects.equal(mInputOptions, column.mInputOptions);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mInputOptions);
  }

  /**
   * Provides the ability to specify InputOptions for a column.
   */
  public static final class InputOptions implements Serializable {
    /** Maximum number of versions of a cell to return. */
    private final int mMaxVersions;
    /** HBase column filter to apply to this column. */
    private final KijiColumnFilter mFilter;

    /**
     * Constructs a new InputOptions.
     *
     * @param maxVersions Maximum number of versions of a cell to return.
     * @param filter HBase column filter to apply to this column.
     */
    public InputOptions(int maxVersions, KijiColumnFilter filter) {
      mMaxVersions = maxVersions;
      mFilter = filter;
    }

    /** @return Maximum number of versions of a cell to return. */
    public int maxVersions() {
      return mMaxVersions;
    }

    /** @return HBase column filter to apply to this column. */
    public KijiColumnFilter filter() {
      return mFilter;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof InputOptions)) {
        return false;
      }

      final InputOptions column = (InputOptions) other;
      return Objects.equal(mMaxVersions, column.mMaxVersions)
          && Objects.equal(mFilter, column.mFilter);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(mMaxVersions, mFilter);
    }
  }
}
