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
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Tracks stateful resources which require manual cleanup.
 *
 * <p>
 *   Resources which require explicit cleanup before being garbage collected may register themselves
 *   with this tracker by calling {@link #registerResource} and unregister by calling
 *   {@link #unregisterResource}. If registered resources are not unregistered before garbage
 *   collection or JVM shutdown the tracker will print log messages to the cleanup log. Note that
 *   the tracker will not cleanup the leaked resources, it will only log the failure to do so.
 *   The granularity of tracking and type of logging is defined by the {@link TrackingLevel} of the
 *   {@code ResourceTracker}, which is configured by the {@value #TRACKING_LEVEL_PROPERTY}
 *   system property.
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class ResourceTracker {
  /**
   * The system property to set to configure the tracking level of this tracker.
   */
  public static final String TRACKING_LEVEL_PROPERTY =
      "org.kiji.commons.ResourceTracker.tracking_level";

  /**
   * The configured tracking level of this tracker. This value is set by the system property
   * specified by TRACKING_LEVEL_PROPERTY, and may be set to any value of the enum
   * {@link org.kiji.commons.ResourceTracker.TrackingLevel}.
   */
  public static final TrackingLevel TRACKING_LEVEL =
      TrackingLevel.valueOf(System.getProperty(TRACKING_LEVEL_PROPERTY, "COUNTER"));
  private static final Logger LOG = LoggerFactory.getLogger(ResourceTracker.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + ResourceTracker.class.getName());
  private static final ResourceTracker SINGLETON = new ResourceTracker();

  /**
   * Get the singleton ResourceTracker.
   *
   * @return the singleton ResourceTracker.
   */
  public static ResourceTracker get() {
    return SINGLETON;
  }

  /**
   * Options for resource tracking granularity.
   *
   * <p>
   *   The {@code TrackingLevel} determines how the {@code ResourceTracker} handles leaked
   *   resources (resources which are registered and never unregistered).
   * </p>
   *
   * <h3>{@code NONE}</h3>
   *
   * <p>
   *   No tracking is performed. This tracking level imposes no performance overhead.
   * </p>
   *
   * <h3>{@code COUNTER}</h3>
   *
   * <p>
   *   The total count of registered resources is tracked. This is the recommended tracking level
   *   to use in production. The total registered resource count is logged at shutdown if the count
   *   is greater than zero.
   * </p>
   *
   * <p>
   *   {@code COUNTER} level tracking imposes minimal performance overhead: a global atomic counter
   *   must be incremented or decremented during each resource registration or deregistration.
   *   Additionally, a JVM shutdown hook is registered when {@code COUNTER} level tracking is used.
   * </p>
   *
   * <h3>{@code REFERENCES}</h3>
   *
   * <p>
   *   Individual registered resources are tracked. This is the recommended tracking level
   *   while developing and debugging. Leaked resources are logged along with an associated message
   *   and the stack trace at the point of their registration.
   * </p>
   *
   * <p>
   *   {@code REFERENCES} level tracking imposes some performance overhead. Registering and
   *   deregistering resources is globally synchronized.  The message and associated stack trace for
   *   currently registered resources use memory.  Weak references are held to currently registered
   *   resources, which may affect garbage collector performance, especially for leaked resources.
   *   Additionally, a JVM shutdown hook is registered when {@code REFERENCES} level tracking is
   *   used.
   * </p>
   */
  public enum TrackingLevel {
    NONE, COUNTER, REFERENCES
  }

  /** Count of tracked resources. */
  private final AtomicInteger mCounter;

  /** Tracks individual resources. */
  private final ReferenceTracker mReferenceTracker;

  /** Initializes the singleton ResourceTracker based on the value of TRACKING_LEVEL. */
  private ResourceTracker() {
    switch(TRACKING_LEVEL) {
      case NONE: {
        mReferenceTracker = null;
        mCounter = null;
        break;
      }
      case COUNTER: {
        mReferenceTracker = null;
        mCounter = new AtomicInteger(0);
        LOG.debug("Registering hook to log number of unclosed resources at shutdown.");
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        break;
      }
      case REFERENCES: {
        mReferenceTracker = new ReferenceTracker();
        mCounter = new AtomicInteger(0);
        LOG.debug("Registering hook to log details of unclosed resources at shutdown.");
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        break;
      }
      default: throw new IllegalStateException(
          String.format("Unknown ResourceTracker.TrackingLevel: %s", TRACKING_LEVEL));
    }
  }

  /** Runs at JVM shutdown and logs warnings for open resources. */
  private final class ShutdownHook extends Thread {
    /** {@inheritDoc} */
    @Override
    public void run() {
      switch(TRACKING_LEVEL) {
        case NONE: break;
        case COUNTER: {
          logCounter();
          break;
        }
        case REFERENCES: {
          logCounter();
          mReferenceTracker.close();
          break;
        }
        default: throw new IllegalStateException(
            String.format("Unknown ResourceTracker.TrackingLevel: %s", TRACKING_LEVEL));
      }
    }
  }

  /** Logs the number of outstanding resources during the shutdown hook. */
  private void logCounter() {
    final String message;
    switch(TRACKING_LEVEL) {
      case COUNTER: {
        message = String.format(
            "Found {} unclosed resources. Run with system property %s=REFERENCES for more details.",
            TRACKING_LEVEL_PROPERTY);
        break;
      }
      case REFERENCES: {
        message =  "Found {} unclosed resources.";
        break;
      }
      default: throw new IllegalStateException(
          String.format("Unknown ResourceTracker.TrackingLevel: %s", TRACKING_LEVEL));
    }

    final int count = mCounter.get();
    if (0 != count) {
      CLEANUP_LOG.error(message, count);
      LOG.error(message, count);
    } else {
      LOG.debug("JVM shutdown with no unclosed resources.");
    }
  }

  /**
   * Registers a resource that should be cleaned up and removed before JVM shutdown. When using
   * reference level tracking, the message and the current stack trace will be logged.
   *
   * @param resource Object which should be cleaned up before JVM shutdown.
   * @param message string which will be logged along with the current stack trace if the resource
   *    is not closed before shutdown. A good example of this error message is the stack trace at
   *    the time the object was created.
   */
  public void registerResource(
      final Object resource,
      final String message
  ) {
    switch(TRACKING_LEVEL) {
      case NONE: break;
      case COUNTER: {
        mCounter.incrementAndGet();
        break;
      }
      case REFERENCES: {
        // Skip two stack frames. One for the exception, one for this method.
        final String stackTrace =
            Joiner.on('\n').join(Iterables.skip(Arrays.asList(new Exception().getStackTrace()), 2));
        mCounter.incrementAndGet();
        mReferenceTracker.registerResource(resource, message, stackTrace);
        break;
      }
      default: throw new IllegalArgumentException(
          String.format("Unknown ResourceTracker.TrackingLevel: %s", TRACKING_LEVEL));
    }
  }

  /**
   * Registers a resource that should be cleaned up and removed before JVM shutdown. When using
   * reference level tracking, the message will be the {@link #toString()} of the resource, and the
   * current stack trace.
   * <p>
   * This method will call {@link #toString()} on the passed in resource, so it must be in a valid
   * state.
   *
   * @param resource Object which should be cleaned up before JVM shutdown.
   */
  public void registerResource(
      final Object resource
  ) {
    switch(TRACKING_LEVEL) {
      case NONE: break;
      case COUNTER: {
        mCounter.incrementAndGet();
        break;
      }
      case REFERENCES: {
        // Skip two stack frames. One for the exception, one for this method.
        final String stackTrace =
            Joiner.on('\n').join(Iterables.skip(Arrays.asList(new Exception().getStackTrace()), 2));

        mCounter.incrementAndGet();
        mReferenceTracker.registerResource(resource, resource.toString(), stackTrace);
        break;
      }
      default: throw new IllegalStateException(
          String.format("Unknown ResourceTracker.TrackingLevel: %s", TRACKING_LEVEL));
    }
  }

  /**
   * Removes tracking from a resource, indicating that it has been successfully cleaned up.
   * <p>
   * This method will call {@link #toString()} on the passed in resource, so it must be in a valid
   * state.
   *
   * @param resource Object which has been cleaned up and need no longer be tracked.
   */
  public void unregisterResource(
      final Object resource
  ) {
    switch(TRACKING_LEVEL) {
      case NONE: break;
      case COUNTER: {
        mCounter.decrementAndGet();
        break;
      }
      case REFERENCES: {
        mCounter.decrementAndGet();
        mReferenceTracker.unregisterResource(resource);
        break;
      }
      default: throw new IllegalStateException(
          String.format("Unknown ResourceTracker.TrackingLevel: %s", TRACKING_LEVEL));
    }
  }


  /**
   * Returns whether the resource is registered with this resource tracker. This is useful for
   * testing interactions with the debug resource tracker.
   *
   * @param resource The resource to test for registration.
   * @return Whether the provided resource is registered.
   * @throws IllegalStateException if the tracking level is not {@code TrackingLevel.REFERENCES}.
   */
  public boolean isResourceRegistered(final Object resource) {
    switch(TRACKING_LEVEL) {
      case REFERENCES: {
        return mReferenceTracker.resourceIsRegistered(resource);
      }
      default: throw new IllegalStateException(String.format(
          "Tracking level must be REFERENCES in order to check if resource is registered."
              + " Current tracking level: %s.", TRACKING_LEVEL));
    }
  }

  /**
   * Tracks registered resources. Uses the phantom reference mechanism of the JVM to recognize when
   * registered resources are no longer reachable, and logs them. Will log any registered resources
   * when closed.
   */
  private static final class ReferenceTracker implements Closeable {
    private final ExecutorService mExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("org.kiji.commons.ResourceTracker.ReferenceTracker")
                .build());

    /** Create a {@code ReferenceTracker} instance. */
    private ReferenceTracker() {
      mExecutorService.execute(new ReferenceLogger());
    }

    /** Ref queue for resource references. */
    private final ReferenceQueue<Object> mReferenceQueue = new ReferenceQueue<Object>();

    /** Map of identity hash of registered resource to reference. Access must be synchronized. */
    private final ListMultimap<Integer, ResourceReference> mReferences = ArrayListMultimap.create();

    /**
     * Register a resource to be tracked.
     *
     * @param resource to be registered to this tracker.
     * @param message associated with the resource.
     * @param stackTrace associated with the resource.
     */
    public void registerResource(
        final Object resource,
        final String message,
        final String stackTrace
    ) {
      LOG.debug("Registering resource {}.", resource);
      final ResourceReference ref =
          new ResourceReference(mReferenceQueue, resource, message, stackTrace);
      synchronized (mReferences) {
        mReferences.put(ref.getIdentityHash(), ref);
      }
    }

    /**
     * Unregister a resource from this tracker.
     *
     * @param resource to be unregistered from this tracker.
     */
    public void unregisterResource(final Object resource) {
      LOG.debug("Unregistering resource {}.", resource);
      synchronized (mReferences) {
        final List<ResourceReference> refs = mReferences.get(System.identityHashCode(resource));
        for (int i = 0; i < refs.size(); i++) {
          final ResourceReference ref = refs.get(i);
          // The referent is guaranteed to be present, because the argument is a strong reference
          if (resource == ref.get()) {
            refs.remove(i);
            return;
          }
        }
      }
      CLEANUP_LOG.warn("Attempted to unregister an untracked resource: {}. At\n{}", resource,
          Joiner.on('\n').join(Iterables.skip(Arrays.asList(new Exception().getStackTrace()), 2)));
    }

    /**
     * Returns whether the resource is registered with this resource tracker. This is mostly
     * useful for testing the reference tracker itself.
     *
     * @param resource The resource to test for registration.
     * @return Whether the provided resource is registered.
     */
    public boolean resourceIsRegistered(final Object resource) {
      synchronized (mReferences) {
        final List<ResourceReference> refs = mReferences.get(System.identityHashCode(resource));
        for (final ResourceReference ref : refs) {
          // The referent is guaranteed to be present, because the argument is a strong reference
          if (resource == ref.get()) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Close this {@code ReferenceTracker}.  Will log any outstanding registered resources.
     */
    @Override
    public void close() {
      mExecutorService.shutdownNow();
      synchronized (mReferences) {
        for (ResourceReference reference : mReferences.values()) {
          logReference(reference);
          reference.clear(); // Prevent the reference from being enqueued
        }
        mReferences.clear();
      }
    }

    /**
     * Log a reference in the cleanup log with it's message.
     *
     * @param reference to log.
     */
    private static void logReference(ResourceReference reference) {
      CLEANUP_LOG.error("Leaked resource detected: {}\n{}",
          reference.getMessage(),
          reference.getStackTrace());
    }

    /** Task which waits for {@code ResourceReference} instances to be enqueued, and logs them. */
    private class ReferenceLogger implements Runnable {
      /** {@inheritDoc} */
      @Override
      public void run() {
        try {
          while (true) {
            ResourceReference ref = (ResourceReference) mReferenceQueue.remove();
            synchronized (mReferences) {
              // Remove multiple times in case of multiple registrations
              while (mReferences.remove(ref.getIdentityHash(), ref)) {
                logReference(ref);
              }
            }
            ref.clear(); // Clear the reference to indicate we are finished with it
          }
        } catch (InterruptedException e) {
          // If this thread is interrupted, then die. This happens normally when
          // ReferenceTracker#close is called.

          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
    }

    /**
     * A {@link java.lang.ref.WeakReference} to a resource which holds an identity hash code,
     * message, and stacktrace for the resource.
     */
    @ApiAudience.Private
    private static final class ResourceReference extends WeakReference<Object> {

      /** The identity hash code of the resource. */
      private final int mIdentityHash;

      /** Message associated with the resource. */
      private final String mMessage;

      /** Stack trace associated with the resource. */
      private final String mStackTrace;

      /**
       * Create a {@code ResourceReference} with the provided queue, resource, message, and
       * stack trace.
       *
       * @param refQueue to which this reference will be enqueued when the JVM determines the
       *    resource is no longer reachable.
       * @param resource to reference.
       * @param message associated with the resource.
       * @param stackTrace associated with the resource.
       */
      public ResourceReference(
          final ReferenceQueue<Object> refQueue,
          final Object resource,
          final String message,
          final String stackTrace
      ) {
        super(resource, refQueue);
        mIdentityHash = System.identityHashCode(resource);
        mMessage = message;
        mStackTrace = stackTrace;
      }

      /**
       * @return the identity hash code of the resource.
       */
      public int getIdentityHash() {
        return mIdentityHash;
      }

      /**
       * @return the message associated with the resource.
       */
      public String getMessage() {
        return mMessage;
      }

      /**
       * @return the stack trace associated with the resource.
       */
      public String getStackTrace() {
        return mStackTrace;
      }

      /** {@inheritDoc} */
      @Override
      public String toString() {
        return Objects
            .toStringHelper(this.getClass())
            .add("identity hash", mIdentityHash)
            .add("message", mMessage)
            .add("stack trace", mStackTrace)
            .toString();
      }
    }
  }

  /**
   * Creates a proxy object to a closeable resource.
   *
   * <p>
   *   All calls to the proxy will be passed through to the underlying resource. The passed in
   *   resource should not be used or closed after creating the proxy. The returned proxy will be an
   *   instance of the given interface.
   * </p>
   *
   * <p>
   *   When the proxy is created the resource is registered with the resource tracker.  When the
   *   proxy is closed, the resource will be unregistered.
   * </p>
   *
   * @param iface The interface that the returned proxy will implement.
   * @param resource The resource to pass through calls to.
   * @param <T> Interface that defines the object to be returned.
   * @param <U> Type of resource provided.
   * @return A tracked proxy to the provided resource.
   */
  public static <T extends Closeable, U extends T> T getTrackedProxy(
      final Class<T> iface,
      final U resource
  ) {
    @SuppressWarnings("unchecked")
    final T proxy = (T) Proxy.newProxyInstance(
        resource.getClass().getClassLoader(),
        new Class[]{iface},
        new TrackedProxy<T>(resource));
    get().registerResource(resource);
    return proxy;
  }

  /**
   * A dynamic proxy that wraps a {@link Closeable} resource.
   *
   * <p>
   *   When the proxy is closed, the resource will be unregistered from the resource tracker.
   * </p>
   *
   * @param <T> Type of resource.
   */
  private static final class TrackedProxy<T extends Closeable> implements InvocationHandler {
    private final T mResource;

    /**
     * Create a tracked resource proxy.
     *
     * @param resource The resource to track.
     */
    private TrackedProxy(final T resource) {
      mResource = resource;
    }

    /** {@inheritDoc} */
    @Override
    public Object invoke(
        final Object proxy,
        final Method method,
        final Object[] args
    ) throws Throwable {
      if (method.getName().equals("close") && args == null) {
        get().unregisterResource(mResource);
      }
      return method.invoke(mResource, args);
    }
  }
}
