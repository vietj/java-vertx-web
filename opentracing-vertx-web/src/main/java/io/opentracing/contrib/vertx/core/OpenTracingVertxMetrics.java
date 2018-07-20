package io.opentracing.contrib.vertx.core;

import io.opentracing.Tracer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.metrics.impl.DummyVertxMetrics;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.HttpServerMetrics;

/**
 * @author Pavol Loffay
 */
public class OpenTracingVertxMetrics extends DummyVertxMetrics {

  private Tracer tracer;

  public OpenTracingVertxMetrics(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public OpenTracingClientMetrics createHttpClientMetrics(HttpClientOptions options) {
    return new OpenTracingClientMetrics(tracer);
  }

  @Override
  public HttpServerMetrics createHttpServerMetrics(HttpServerOptions options, SocketAddress localAddress) {
    return new OpenTracingServerMetrics(tracer);
  }
}
