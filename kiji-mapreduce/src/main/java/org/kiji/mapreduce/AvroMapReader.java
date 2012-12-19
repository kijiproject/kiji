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

package org.kiji.mapreduce;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.hadoop.util.AvroCharSequenceComparator;

/**
 * A reader for Avro "map" data.
 *
 * <p>Avro "map" types are implemented in Java as {@link java.util.Map}, where the keys
 * are {@link java.lang.CharSequence}.  Since <code>CharSequence</code> does not refine
 * the general contracts of <code>equals()</code> and <code>hashCode()</code> methods,
 * this unfortunately means that testing for the existence of a key in an Avro map is not
 * possible.  In particular, if you test for the existence of a <code>String</code> key
 * <code>"foo"</code> in a map deserialized using Avro, its underyling
 * <code>HashMap</code> implementation will not match your <code>"foo"</code> hash code to
 * its underlying <code>Utf8("foo")</code>'s hash code.</p>
 *
 * <p>A possible workaround would be to always use <code>Utf8</code> objects whenever
 * working with Avro maps.  However, this is not possible when writing framework code like
 * Kiji.  Clients must be free to use any <code>CharSequence</code> implementation of
 * their choosing.</p>
 *
 * <p>Until this issue is resolved in Avro, use this class to read from Avro "map"
 * types.  This is a read-only map.  Calling a method that would mutate the map will throw
 * an <code>UnsupportedOperationException</code>.  If the underlying Avro "map" is
 * mutated, you should call <code>read()</code> to reread the map.
 *
 * @param <V> The map's value type.
 */
public class AvroMapReader<V> implements Map<CharSequence, V> {
  /** The wrapped Avro map to read. */
  private final Map<CharSequence, V> mMap;

  /**
   * A map that mirrors the data in the wrapped Avro map.
   *
   * <p>This map has a key comparator that correctly compares CharSequences, so comparing a
   * String with a Utf8 works as expected.</p>
   */
  private Map<CharSequence, V> mMirror;

  /**
   * Wraps an Avro map.
   *
   * @param map The Avro map to read from.
   */
  public AvroMapReader(Map<CharSequence, V> map) {
    mMap = map;
    mMirror = null;
  }

  /** Reads the wrapped Avro map into a Map with a comparator for CharSequences. */
  public void read() {
    mMirror = new TreeMap<CharSequence, V>(AvroCharSequenceComparator.INSTANCE);
    mMirror.putAll(mMap);
  }

  /**
   * Gets the mirrored Avro map that works with any CharSequence key.
   *
   * <p>This method will read the wrapped Avro map into the mirror if necessary.</p>
   *
   * @return The mirrored Avro map.
   */
  private Map<CharSequence, V> getMirror() {
    if (null == mMirror) {
      read();
    }
    return mMirror;
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsKey(Object key) {
    return getMirror().containsKey(key);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsValue(Object value) {
    return mMap.containsValue(value);
  }

  /** {@inheritDoc} */
  @Override
  public Set<Map.Entry<CharSequence, V>> entrySet() {
    return getMirror().entrySet();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    return getMirror().equals(o);
  }

  /** {@inheritDoc} */
  @Override
  public V get(Object key) {
    return getMirror().get(key);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return getMirror().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isEmpty() {
    return mMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public Set<CharSequence> keySet() {
    return getMirror().keySet();
  }

  /** {@inheritDoc} */
  @Override
  public V put(CharSequence key, V value) {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public void putAll(Map<? extends CharSequence, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public int size() {
    return mMap.size();
  }

  /** {@inheritDoc} */
  @Override
  public Collection<V> values() {
    return mMap.values();
  }
}
