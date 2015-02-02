package org.kiji.rest.monitoring;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRiemannReporterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestRiemannReporterFactory.class);

  @Test
  public void isDiscoverable() throws Exception {
    Assert.assertTrue(
        new DiscoverableSubtypeResolver()
            .getDiscoveredSubtypes()
            .contains(RiemannReporterFactory.class));
  }
}
