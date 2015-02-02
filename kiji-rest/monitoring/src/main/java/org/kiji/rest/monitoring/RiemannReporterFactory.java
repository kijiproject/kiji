package org.kiji.rest.monitoring;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.aphyr.riemann.client.TcpTransport;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.riemann.Riemann;
import com.codahale.metrics.riemann.RiemannReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.metrics.BaseReporterFactory;
import io.dropwizard.util.Duration;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import org.kiji.commons.SocketAddressUtils;

/**
 * A factory for {@link RiemannReporter} instances.
 * <p/>
 * <b>Configuration Parameters:</b>
 * <table>
 *     <tr>
 *         <td>Name</td>
 *         <td>Default</td>
 *         <td>Description</td>
 *     </tr>
 *     <tr>
 *         <td>host</td>
 *         <td>localhost</td>
 *         <td>The hostname of the Riemann server to report to.</td>
 *     </tr>
 *     <tr>
 *         <td>port</td>
 *         <td>8080</td>
 *         <td>The port of the Riemann server to report to.</td>
 *     </tr>
 *     <tr>
 *         <td>prefix</td>
 *         <td><i>None</i></td>
 *         <td>The prefix for Metric key names to report to Riemann.</td>
 *     </tr>
 * </table>
 */
@JsonTypeName("riemann")
public class RiemannReporterFactory extends BaseReporterFactory {

  @NotEmpty
  private String mHost = "localhost";

  @Range(min = 0, max = 49151)
  private int mPort = TcpTransport.DEFAULT_PORT;

  private String mPrefix = null;

  @JsonProperty
  public String getHost() {
    return mHost;
  }

  @JsonProperty
  public void setHost(String host) {
    mHost = host;
  }

  @JsonProperty
  public int getPort() {
    return mPort;
  }

  @JsonProperty
  public void setPort(int port) {
    mPort = port;
  }

  @JsonProperty
  public String getPrefix() {
    return mPrefix == null || mPrefix.isEmpty() ? "kiji.rest" : "kiji.rest." + mPrefix;
  }

  @JsonProperty
  public void setPrefix(String prefix) {
    mPrefix = prefix;
  }

  @Override
  public RiemannReporter build(MetricRegistry registry) {
    final Duration frequency = getFrequency().or(Duration.seconds(5));
    try {
      final RiemannReporter reporter = RiemannReporter
          .forRegistry(registry)
          .localHost(SocketAddressUtils.getPublicLocalHost().getHostName())
          .prefixedWith(getPrefix())
          .withTtl(60.0f)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .useSeparator(".")
          .build(new Riemann(getHost(), getPort()));
      reporter.start(frequency.getQuantity(), frequency.getUnit());
      return reporter;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
