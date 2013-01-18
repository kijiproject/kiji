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

package org.kiji.testing.fakehtable

import org.apache.commons.codec.binary.Hex

/** Helper to work with the MD5 value space. */
object MD5Space {
  val Min = BigInt(0)
  val Max = (BigInt(1) << 128) - 1

  private val MaxLong = 0x7fffffffffffffffL

  /**
   * Maps a position from 0.0 to 1.0 into the MD5 hash space.
   *
   * @param pos Position in the space, from 0.0 to 1.0
   * @return MD5 hash as an array of 16 bytes.
   */
  def apply(pos: Double): Array[Byte] = {
    val hash = Min + ((Max * (pos * MaxLong).toLong) / MaxLong)
    var hashStr = "%032x".format(hash)
    return Hex.decodeHex(hashStr.toCharArray)
  }

  /**
   * Maps a position from a rational number into the MD5 hash space.
   *
   * @param num Numerator (≥ 0 and ≤ denum).
   * @param denum Denumerator (≥ 0).
   * @return MD5 hash as an array of 16 bytes.
   */
  def apply(num: BigInt, denum: BigInt): Array[Byte] = {
    require((num >= 0) && (denum > 0))
    require(num <= denum)
    val hash = Min + ((num * Max) / denum)
    var hashStr = "%032x".format(hash)
    return Hex.decodeHex(hashStr.toCharArray)
  }
}
