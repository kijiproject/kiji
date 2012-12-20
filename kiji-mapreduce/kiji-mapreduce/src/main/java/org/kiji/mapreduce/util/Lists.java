/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A library of static List manipulation functions. */
public final class Lists {
  /** Private constructor to prevent instantiation. */
  private Lists() {
  }

  /**
   * Aggregation function to apply over a list of elements of type X with foldLeft()
   * or foldRight(). The aggregation returns an element of type Y.
   *
   * @param <X> the type of the elements of the input list.
   * @param <Y> the result type of the aggregation.
   */
  public abstract static class Aggregator<X, Y> {
    /**
     * Perform an operation on the current list element and intermediate datum.
     * Returns a new intermediate datum, which is the final datum for the entire
     * computation if this is operating on the last element of the list.
     *
     * @param listElem the list element being operated on.
     * @param intermediate an intermediate datum being operated on.
     * @return the next intermediate datum.
     */
    public abstract Y eval(X listElem, Y intermediate);
  }

  /**
   * Generic function to apply to elements of a list.
   *
   * @param <X> the type of the elements of the input list.
   * @param <Y> the result type of the function.
   */
  public abstract static class Func<X, Y> {
    /**
     * Perform an operation on the current list element and return an output value.
     *
     * @param listElem the input list element to consider.
     * @return an output value from some computation.
     */
    public abstract Y eval(X listElem);
  }

  /**
   * Evaluation function that returns True if it encounters a null input item.
   *
   * @param <X> the input argument type.
   */
  public static class IsNullFn<X> extends Func<X, Boolean> {
    /**
     * Returns listItem == null.
     *
     * @param listItem the list item to operate on.
     * @return true if listItem is null, false otherwise.
     */
    @Override
    public Boolean eval(X listItem) {
      return (null == listItem) ? Boolean.TRUE : Boolean.FALSE;
    }
  }

  /** Evaluation function that returns the String representation of each input item. */
  public static class ToStringFn<X> extends Func<X, String> {
    /**
     * Returns listItem.toString().
     *
     * @param listItem the list item to operate on.
     * @return listItem.toString(), or null if listItem is null.
     */
    @Override
    public String eval(X listItem) {
      if (null == listItem) {
        return null;
      }
      return listItem.toString();
    }
  }

  /**
   * Operate element-wise on elements of a list in a left-to-right fashion,
   * computing an aggregated value. Returns the final aggregated value after
   * computing over all the elements of the list.
   *
   * @param <X> the type of the input list elements.
   * @param <Y> the type of the aggregated return value.
   * @param initial the initial value of the accumulator to apply with inputList[0].
   * @param inputList the input elements to compute over.
   * @param op the operation to perform on each (element, intermediateVal) pair.
   * @return initial if the list is null or empty, or the result of
   *     op(lst[n], op(lst[n-1], ..., (op(lst[1], op(lst[0], initial))))).
   */
  public static <X, Y> Y foldLeft(Y initial, List<X> inputList, Aggregator<? super X, Y> op) {
    if (null == inputList) {
      return initial;
    }

    Y next = initial;
    for (X val : inputList) {
      next = op.eval(val, next);
    }

    return next;
  }

  /**
   * Operate element-wise on elements of a list in a right-to-left fashion,
   * computing an aggregated value. Returns the final aggregated value after
   * computing over all the elements of the list.
   *
   * @param <X> the type of the input list elements.
   * @param <Y> the type of the aggregated return value.
   * @param initial the initial value of the accumulator to apply with inputList[len-1].
   * @param inputList the input elements to compute over.
   * @param op the operation to perform on each (element, intermediateVal) pair.
   * @return initial if the list is null or empty, or the result of
   *     op(lst[0], op(lst[1], ..., (op(lst[n-2], op(lst[n-1], initial))))).
   */
  public static <X, Y> Y foldRight(Y initial, List<X> inputList, Aggregator<? super X, Y> op) {
    if (null == inputList || inputList.isEmpty()) {
      return initial;
    }

    Y next = initial;
    for (int i = inputList.size() - 1; i >= 0; i--) {
      X val = inputList.get(i);
      next = op.eval(val, next);
    }

    return next;
  }

  /**
   * Consider elements of a list, returning true if one of the elements of the list
   * satisfies the predicate function so that it returns True. This may short circuit
   * evaluation if an element of the list satisfies the predicate.
   *
   * @param <X> the input array element type.
   * @param inputList a list of elements to apply a truth predicate to.
   * @param predicate a function that returns True or False for each element of the input list.
   * @return true if predicate(inputList[i]) returns true for any index 'i'.
   */
  public static <X> boolean exists(List<X> inputList, Func<? super X, Boolean> predicate) {
    if (null == inputList || inputList.isEmpty()) {
      return false;
    }

    for (X val : inputList) {
      Boolean ret = predicate.eval(val);
      if (null == ret) {
        throw new RuntimeException("predicate to exists() must return True or False");
      } else if (Boolean.TRUE.equals(ret)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Applies the mapper function element-by-element, creating an output list of the same
   * length as the input list, with the transformation function applied to each.
   *
   * @param <X> the input array element type.
   * @param <Y> the output array element type.
   * @param inputs an input list of elements to transform.
   * @param mapper a function to apply to each element one-by-one
   * @return a list of the same length as the input list, containing
   *     [mapper.eval(inputs[0]), mapper.eval(inputs[1]), ..., mapper.eval(inputs[n-1])].
   */
  public static <X, Y> List<Y> map(List<X> inputs, Func<? super X, ? extends Y> mapper) {
    if (null == inputs) {
      return null;
    } else if (inputs.isEmpty()) {
      return Collections.emptyList();
    }

    List<Y> out = new ArrayList<Y>();
    for (X in : inputs) {
      out.add(mapper.eval(in));
    }

    return out;
  }

  /**
   * Returns an array of the same length as the input list.
   * If the input list is null, returns a zero-length array.
   *
   * @param <X> the input array element type.
   * @param inputs the input list to convert to an array.
   * @param klazz the Class associated with the array element type.
   * @return an array containing all the elements of the input list.
   */
  @SuppressWarnings("unchecked")
  public static <X> X[] toArray(List<X> inputs, Class<X> klazz) {
    if (null == inputs) {
      return (X[]) Array.newInstance(klazz, 0);
    }

    X[] out = inputs.toArray((X[]) Array.newInstance(klazz, inputs.size()));
    return out;
  }

  /**
   * Returns a list containing only the distinct elements of the input list.
   * The .equals() and .hashCode() methods must be implemented for the input
   * elements. (The 'null' value is also permitted.)
   *
   * @param <X> the type of the input elements.
   * @param inputs a list of input elements.
   * @return a list of the same values as 'inputs', but deduplicated. Elements
   *     remain in the same order as they appeared in the input list.
   */
  public static <X> List<X> distinct(List<X> inputs) {
    if (null == inputs) {
      return null;
    }

    return foldLeft(new ArrayList<X>(), inputs, new Aggregator<X, List<X>>() {
      private Set<X> mSeen = new HashSet<X>();

      @Override
      public List<X> eval(X input, List<X> out) {
        if (!mSeen.contains(input)) {
          out.add(input);
          mSeen.add(input);
        }
        return out;
      }
    });
  }
}


