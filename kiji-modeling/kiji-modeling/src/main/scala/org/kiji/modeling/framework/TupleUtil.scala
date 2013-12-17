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

package org.kiji.modeling.framework

import scala.collection.JavaConverters.asScalaIteratorConverter

import cascading.tuple.Fields

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * The TupleUtil object contains various convenience functions for dealing with scala and
 * Cascading/Scalding tuples.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object TupleUtil {
  /**
   * Converts a Cascading fields object into a sequence of the fields it contains. This only
   * supports fields without a special [[cascading.tuple.Fields.Kind]].
   *
   * @param fields to convert.
   * @return a sequence of field names.
   */
  def fieldsToSeq(fields: Fields): Seq[String] = {
    fields
        .iterator()
        .asScala
        .map { field => field.toString }
        .toSeq
  }

  /**
   * Converts a tuple into an appropriate representation for processing by a model phase function.
   * Handles instances of Tuple1 as special cases and unpacks them to permit functions with only one
   * parameter to be defined without expecting their argument to be wrapped in a Tuple1 instance.
   *
   * @tparam T is the type of the output function argument.
   * @param tuple to convert.
   * @return an argument ready to be passed to a model phase function.
   */
  def tupleToFnArg[T](tuple: Product): T = {
    tuple match {
      case Tuple1(x1) => x1.asInstanceOf[T]
      case other => other.asInstanceOf[T]
    }
  }

  /**
   * Converts a function return value into a tuple. Handles the case where the provided result is
   * not a tuple by wrapping it in a Tuple1 instance.
   *
   * @param result from a model phase function.
   * @return a processed tuple.
   */
  def fnResultToTuple(result: Any): Product = {
    result match {
      case tuple: Tuple1[_] => tuple
      case tuple: Tuple2[_, _] => tuple
      case tuple: Tuple3[_, _, _] => tuple
      case tuple: Tuple4[_, _, _, _] => tuple
      case tuple: Tuple5[_, _, _, _, _] => tuple
      case tuple: Tuple6[_, _, _, _, _, _] => tuple
      case tuple: Tuple7[_, _, _, _, _, _, _] => tuple
      case tuple: Tuple8[_, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple9[_, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple10[_, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple11[_, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple12[_, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple13[_, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple14[_, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case tuple: Tuple22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] => tuple
      case other => Tuple1(other)
    }
  }

  /**
   * Converts a sequence to a tuple.
   *
   * @tparam T is the type of the output tuple.
   * @param sequence to convert.
   * @return a tuple converted from the provided sequence.
   */
  def seqToTuple[T <: Product](sequence: Seq[_]): T = {
    val tuple = sequence match {
      case Seq(x1) => {
        Tuple1(x1)
      }
      case Seq(x1, x2) => {
        Tuple2(x1, x2)
      }
      case Seq(x1, x2, x3) => {
        Tuple3(x1, x2, x3)
      }
      case Seq(x1, x2, x3, x4) => {
        Tuple4(x1, x2, x3, x4)
      }
      case Seq(x1, x2, x3, x4, x5) => {
        Tuple5(x1, x2, x3, x4, x5)
      }
      case Seq(x1, x2, x3, x4, x5, x6) => {
        Tuple6(x1, x2, x3, x4, x5, x6)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7) => {
        Tuple7(x1, x2, x3, x4, x5, x6, x7)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8) => {
        Tuple8(x1, x2, x3, x4, x5, x6, x7, x8)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9) => {
        Tuple9(x1, x2, x3, x4, x5, x6, x7, x8, x9)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) => {
        Tuple10(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11) => {
        Tuple11(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12) => {
        Tuple12(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13) => {
        Tuple13(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14) => {
        Tuple14(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15) => {
        Tuple15(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16) => {
        Tuple16(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17) => {
        Tuple17(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18) => {
        Tuple18(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19) => {
        Tuple19(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19, x20) => {
        Tuple20(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19, x20, x21) => {
        Tuple21(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20, x21)
      }
      case Seq(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
          x19, x20, x21, x22) => {
        Tuple22(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20, x21, x22)
      }
    }

    tuple.asInstanceOf[T]
  }
}
