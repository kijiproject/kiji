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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.google.common.base.Function;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * A keyed cache which keeps track of how many outstanding users of the cached value exist.
 *
 * <p>
 *   When the values returned by {@link #get} are closed, their reference count is automatically
 *   decremented in the cache. When the reference count falls to 0, the cached value (which must
 *   implement {@link Closeable}), is removed from the cache and closed.
 * </p>
 *
 * <p>
 *   In general, this class is an easier to use version of {@link ReferenceCountedCache} (since
 *   values need not be manually returned), but it has a few caveats:
 *   <ul>
 *     <li>The value type must be an interface that extends {@code Closeable}.
 *     <li>The values loaned out by this cache have a small method call performance overhead.
 *     <li>Unless equality for the underlying class is based only on the interface methods, proxies
 *       with the same underlying value will not be equal.
 *   </ul>
 * </p>
 *
 * <p>
 *   The {@code ReferenceCountedProxyCache} may optionally be closed, which will preemptively close
 *   all cached entries regardless of reference count. See the javadoc of {@link #close()} for
 *   caveats.
 * </p>
 *
 * @param <K> key type of cache.
 * @param <V> value type of cache. Must be an interface.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class ReferenceCountedProxyCache<K, V extends Closeable> implements Closeable {

  private final ReferenceCountedCache<K, V> mCache;
  private final Class[] mIface;
  private final boolean mTrackProxies;

  /**
   * Private default constructor.
   *
   * @param iface The interface class of values in this cache.
   * @param cacheLoader A function to compute cached values on demand. Should never return null.
   * @param trackResources Whether the cached values should be registered with the resource tracker.
   * @param trackProxies Whether the created proxy values should be registered with the resource
   *    tracker.
   */
  public ReferenceCountedProxyCache(
      final Class<V> iface,
      final Function<? super K, ? extends V> cacheLoader,
      final boolean trackResources,
      final boolean trackProxies
  ) {
    if (trackResources) {
      mCache = ReferenceCountedCache.createWithResourceTracking(cacheLoader);
    } else {
      mCache = ReferenceCountedCache.create(cacheLoader);
    }
    mIface = new Class[] { iface };
    mTrackProxies = trackProxies;
  }

  /**
   * Create a {@code ReferenceCountedProxyCache} using the supplied function to create cached values
   * on demand.
   *
   * <p>
   *   The function may be evaluated with the same key multiple times in the case that the key
   *   becomes invalidated due to the reference count falling to 0. A function need not handle the
   *   case of a null key, but it should never return a null value for any key.
   * </p>
   *
   * @param iface The interface class of values in this cache.
   * @param cacheLoader A function to compute cached values on demand. Should never return null.
   * @return A new {@link ReferenceCountedCache}.
   * @param <K> The key type of cache.
   * @param <V> The value type (must implement {@link java.io.Closeable}) of cache.
   */
  public static <K, V extends Closeable> ReferenceCountedProxyCache<K, V> create(
      final Class<V> iface,
      final Function<? super K, ? extends V> cacheLoader
  ) {
    return new ReferenceCountedProxyCache<>(iface, cacheLoader, false, false);
  }

  /**
   * Create a {@code ReferenceCountedCache} using the supplied function to create cached values
   * on demand with resource tracking of cached values.
   *
   * <p>
   *   The function may be evaluated with the same key multiple times in the case that the key
   *   becomes invalidated due to the reference count falling to 0. A function need not handle the
   *   case of a null key, but it should never return a null value for any key.
   * </p>
   *
   * <p>
   *   The cached values will be registered with the {@link ResourceTracker}, and automatically
   *   unregistered when their reference count falls to 0 and they are removed from the cache. The
   *   values will be registered once upon creation, not every time the value is borrowed. If the
   *   value objects already perform resource tracking, then prefer using {@link #create} to avoid
   *   double registration.
   * </p>
   *
   * @param iface The interface class of values in this cache.
   * @param cacheLoader A function to compute cached values on demand. Should never return null.
   * @return A new {@link ReferenceCountedCache}.
   * @param <K> The key type of the cache.
   * @param <V> The value type (must implement {@link java.io.Closeable}) of the cache.
   */
  public static <K, V extends Closeable> ReferenceCountedProxyCache<K, V>
  createWithResourceTracking(
      final Class<V> iface,
      final Function<? super K, ? extends V> cacheLoader
  ) {
    return new ReferenceCountedProxyCache<>(iface, cacheLoader, true, false);
  }

  /**
   * Create a {@code ReferenceCountedCache} using the supplied function to create cached values
   * on demand with resource tracking of proxies.
   *
   * <p>
   *   The function may be evaluated with the same key multiple times in the case that the key
   *   becomes invalidated due to the reference count falling to 0. A function need not handle the
   *   case of a null key, but it should never return a null value for any key.
   * </p>
   *
   * <p>
   *   The returned proxy objects will be registered with the {@link ResourceTracker}, and
   *   automatically unregistered when they are closed. This is useful for debugging resource
   *   leaks, but comes at the cost of more actively tracked resources. Prefer
   *   {@link #createWithResourceTracking} for performance-sensitive caches that need tracking.
   * </p>
   *
   * @param iface The interface class of values in this cache.
   * @param cacheLoader A function to compute cached values on demand. Should never return null.
   * @return A new {@link ReferenceCountedCache}.
   * @param <K> The key type of the cache.
   * @param <V> The value type (must implement {@link java.io.Closeable}) of the cache.
   */
  public static <K, V extends Closeable> ReferenceCountedProxyCache<K, V> createWithProxyTracking(
      final Class<V> iface,
      final Function<? super K, ? extends V> cacheLoader
  ) {
    return new ReferenceCountedProxyCache<>(iface, cacheLoader, false, true);
  }

  /**
   * Returns the value associated with {@code key} in this cache, first creating that value if
   * necessary. No observable state associated with this cache is modified until loading completes.
   *
   * @param key for which to retrieve cached value.
   * @return cached value associated with the key.
   */
  @SuppressWarnings("unchecked")
  public V get(final K key) {
    final V value = mCache.get(key);
    final ReferenceCountedProxy<K, V> proxy =
        new ReferenceCountedProxy<>(value, key, mCache, mTrackProxies);
    if (mTrackProxies) {
      ResourceTracker.get().registerResource(proxy);
    }
    return (V) Proxy.newProxyInstance(value.getClass().getClassLoader(), mIface, proxy);
  }

  /**
   * Returns whether this cache contains <i>or</i> contained a cached entry for the provided key.
   *
   * @param key to check for a cached entry.
   * @return whether this cache contains <i>or</i> contained a cached entry for the key.
   */
  public boolean containsKey(final K key) {
    return mCache.containsKey(key);
  }

  /**
   * Make a best effort at closing all the cached values. This method is *not* guaranteed to close
   * every cached value if there are concurrent users of the cache.  As a result, this method
   * should only be relied upon if only a single thread is using this cache while {@code #close} is
   * called.
   *
   * @throws IOException if any entry throws an IOException while closing. An entry throwing an
   *    IOException will prevent any further entries from being closed.
   */
  @Override
  public void close() throws IOException {
    mCache.close();
  }

  /**
   * A dynamic proxy that wraps a {@link Closeable} value.
   *
   * <p>
   *   When the proxy is closed, it will automatically release itself from the provided
   *   {@code ReferenceCountedCache}.
   * </p>
   *
   * <p>
   *   The proxy may optionally unregister itself with the resource tracker when closed.
   * </p>
   *
   * @param <V> Type of underlying value.
   */
  private static final class ReferenceCountedProxy<K, V extends Closeable>
      implements InvocationHandler {
    private final V mValue;
    private final K mKey;
    private final ReferenceCountedCache<K, V> mCache;
    private final boolean mTrackedProxy;

    /**
     * Create a reference counted proxy.
     *
     * @param value The underlying value to proxy calls to.
     * @param key The key of the proxy in the cache.
     * @param cache The reference counted cache.
     * @param trackedProxy Whether this proxy should unregister itself from the resource tracker on
     *    close.
     */
    private ReferenceCountedProxy(
        final V value,
        final K key,
        final ReferenceCountedCache<K, V> cache,
        final boolean trackedProxy
    ) {
      mValue = value;
      mKey = key;
      mCache = cache;
      mTrackedProxy = trackedProxy;
    }

    /** {@inheritDoc} */
    @Override
    public Object invoke(
        final Object proxy,
        final Method method,
        final Object[] args
    ) throws Throwable {
      if (method.getName().equals("close") && args == null) {
        if (mTrackedProxy) {
          ResourceTracker.get().unregisterResource(this);
        }
        mCache.release(mKey);
        return this;
      } else {
        return method.invoke(mValue, args);
      }
    }
  }
}
