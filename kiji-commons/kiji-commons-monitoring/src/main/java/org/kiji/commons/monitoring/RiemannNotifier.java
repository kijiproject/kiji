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

package org.kiji.commons.monitoring;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.client.RiemannClient;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.commons.ResourceTracker;
import org.kiji.commons.SocketAddressUtils;

/**
 * A notifier which sends all notifications to a Riemann instance.
 */
public final class RiemannNotifier implements Notifier {
  private static final Logger LOG = LoggerFactory.getLogger(RiemannNotifier.class);

  /** Number of notifications to queue before dropping. */
  private static final int QUEUE_LENGTH = 1024;

  private final ArrayBlockingQueue<Event> mQueue;

  /** Limit queue-full logging rate to 1 per 10 seconds. */
  private final RateLimiter mQueueFullLimiter = RateLimiter.create(0.1);

  /** Limit Riemann connection IO error logging rate to 1 per 10 seconds. */
  private final RateLimiter mIoExceptionLimiter = RateLimiter.create(0.1);

  /** The hostname of this service. */
  private final String mHost;

  private final ExecutorService mExecutor;

  private final RiemannClient mRiemann;

  /**
   * Construct a new Riemann notifier, which will send all notifications to Riemann server.
   *
   * @param queue Queue of notifications.
   * @param executor Executor on which to perform notification sending.
   * @param riemann The Riemann client.
   * @param host The hostname of this service.
   */
  private RiemannNotifier(
      final ArrayBlockingQueue<Event> queue,
      final ExecutorService executor,
      final RiemannClient riemann,
      final String host
  ) {
    mQueue = queue;
    mExecutor = executor;
    mRiemann = riemann;
    mHost = host;

    ResourceTracker.get().registerResource(this);
    mExecutor.submit(new NotifierLoop());
  }

  /**
   * Construct a new Riemann notifier, which will send all notifications to Riemann server.
   *
   * @param hostAddress The hostname of this service.
   * @param riemannAddress The address of the Riemann server.
   * @return A new Riemann notifier.
   * @throws IOException On unrecoverable IO error.
   */
  public static RiemannNotifier create(
      final InetSocketAddress hostAddress,
      final InetSocketAddress riemannAddress
  ) throws IOException {
    final RiemannClient riemann = RiemannClient.tcp(riemannAddress);

    // Get the hostname of this machine. Graphite uses '.' as a separator, so all instances in the
    // hostname are replaced with '_'.
    final String host =
        SocketAddressUtils.localToPublic(hostAddress).getHostName().replace('.', '_')
            + ":" + hostAddress.getPort();

    final ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(QUEUE_LENGTH);

    final ExecutorService executor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("riemann-notifier-%s")
            .build());

    return new RiemannNotifier(queue, executor, riemann, host);
  }

  /** {@inheritDoc} */
  @Override
  public void error(
      final String action,
      final Map<String, String> attributes,
      final Throwable error
  ) {
    final Event.Builder event = Proto.Event.newBuilder();
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      event.addAttributes(
          Proto
              .Attribute
              .newBuilder()
              .setKey(entry.getKey())
              .setValue(entry.getValue())
              .build());
    }

    event
        .setHost(mHost)
        .setService("wibi.bblocks." + action)
        .setTime(System.currentTimeMillis() / 1000L)
        .setState("error")
        .addTags("error")
        .addTags("notification")
        .build();

    if (error != null) {
      final List<StackTraceElement> frames = Arrays.asList(error.getStackTrace());

      final StringBuilder description = new StringBuilder();

      description.append(error.getMessage());
      description.append('\n');
      Joiner.on('\n').appendTo(description, Iterables.limit(frames, 20));
      if (frames.size() > 20) {
        description.append("\n...");
      }
      event.setDescription(description.toString());
    }

    if (!mQueue.offer(event.build()) && mQueueFullLimiter.tryAcquire()) {
      LOG.error("Riemann queue full. Dropping notifications.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void error(
      final String action,
      final Map<String, String> attributes
  ) {
    error(action, attributes, null);
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    ResourceTracker.get().unregisterResource(this);
    mExecutor.shutdownNow();
    try {
      if (!mExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Unable to shut down RiemannNotifier within timeout.");
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
  }

  /**
   * A runnable loop which waits for notifications on the queue, and sends them to Riemann.
   */
  private final class NotifierLoop implements Runnable {
    /** {@inheritDoc} */
    @Override
    public void run() {
      final List<Proto.Event> events = Lists.newArrayListWithCapacity(QUEUE_LENGTH);

      while (true) {
        // We connect/disconnect to Riemann in every loop, since notifications should be fairly rare
        // in production services. This keeps the normal-case overhead of the notifier low in
        // exchange for slightly increasing the cost of notifications.

        try {
          events.add(mQueue.take()); // Block waiting for an event
          mRiemann.connect();
          try { // Nested try is necessary because `disconnect` can throw
            mQueue.drainTo(events); // Grab the rest of the waiting events

            LOG.debug("Sending {} notifications to Riemann.", events.size());
            if (!mRiemann.sendEventsWithAck(events)) {
              LOG.warn("Unable to send {} events to Riemann.", events.size());
            }
          } finally {
            events.clear();
            mRiemann.disconnect();
          }
        } catch (InterruptedException e) {
          // Happens normally on close
          Thread.interrupted();
          break;
        } catch (IOException e) {
          if (mIoExceptionLimiter.tryAcquire()) {
            LOG.warn(
                "IOException while sending notifications to Riemann. Dropping notifications.", e);
          }
        } catch (Error e) {
          LOG.error("Error while sending notifications to Riemann. Notifying thread will die.", e);
          break;
        }
      }
    }
  }
}
