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

import com.google.common.base.Objects;

/**
 * This class serves as a light wrapper around a stored value,
 * containing information about when it is instantiated.
 *
 * @param <T> The type that will be stored.
 */
public final class WithTimestamp<T> {
  private final long mTimestamp;
  private final T mValue;

  /**
   * Private constructor. See static factory constructors for
   * description of parameters.
   *
   * @param object The object that will be wrapped.
   * @param timestamp Specifies the time this object was created in milliseconds
   *     since the epoch.
   */
  private WithTimestamp(
      final T object,
      final long timestamp
  ) {
    mTimestamp = timestamp;
    mValue = object;
  }

  /**
   * This static factory constructor allows the user to specify a time of
   * creation.
   *
   * @param object The object that will be wrapped.
   * @param timestamp Specifies the time this object was created in milliseconds
   *     since the epoch.
   * @return A new WithTimestamp instance.
   * @param <U> The type that will be stored.
   */
  public static <U> WithTimestamp<U> create(
      final U object,
      final long timestamp
  ) {
    return new WithTimestamp<U>(object, timestamp);
  }

  /**
   * This static factory constructor returns a new WithTimestamp instance
   * with the default time set to the system time (measured in milliseconds
   * since the epoch) when the function is called.
   *
   * @param object The object that will be wrapped.
   * @return A new WithTimestamp instance.
   * @param <U> The type that will be stored.
   */
  public static <U> WithTimestamp<U> create(
      final U object
  ) {
    final long time = System.currentTimeMillis();
    return new WithTimestamp<U>(object, time);
  }

  /**
   * Get the value stored in this WithTimestamp.
   *
   * @return The value stored in this WithTimestamp.
   */
  public T getValue() {
    return mValue;
  }

  /**
   * Get the creation time associated with the wrapped object.
   *
   * @return The creation time associated with the wrapped object.
   */
  public long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Overrides equals method based on comparing stored timestamp and value.
   *
   * @param o Object that will be tested for equality.
   * @return True if timestamps and values match.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WithTimestamp that = (WithTimestamp) o;

    return (Objects.equal(mTimestamp, that.mTimestamp))
        && (Objects.equal(mValue, that.mValue));
  }

  /**
   * Assigns hashcode based on timestamp and value.
   *
   * @return Haschode based on timestamp and value.
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(mValue, mTimestamp);
  }

  /**
   * Overrides toString method.
   *
   * @return String representation of object.
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("timestamp", mTimestamp)
        .add("value", mValue)
        .toString();
  }
}