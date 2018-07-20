package io.opentracing.contrib.vertx.core;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.contrib.vertx.ext.web.MultiMapInjectAdapter;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.tag.Tags;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.WebSocket;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.HttpClientMetrics;

/**
 * @author Pavol Loffay
 */
public class OpenTracingClientMetrics  implements HttpClientMetrics<Scope, String, String, Void, Void> {

  private final Tracer tracer;

  public OpenTracingClientMetrics(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public Void createEndpoint(String host, int port, int maxPoolSize) {
    return null;
  }

  @Override
  public void closeEndpoint(String host, int port, Void endpointMetric) {
  }

  @Override
  public Void enqueueRequest(Void endpointMetric) {
    return null;
  }

  @Override
  public void dequeueRequest(Void endpointMetric, Void taskMetric) {
  }

  @Override
  public void endpointConnected(Void endpointMetric, String socketMetric) {
  }

  @Override
  public void endpointDisconnected(Void endpointMetric, String socketMetric) {
  }

  @Override
  public Scope requestBegin(Void endpointMetric, String socketMetric,
      SocketAddress localAddress, SocketAddress remoteAddress, HttpClientRequest request) {
    Scope scope = tracer.buildSpan(request.method().name())
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.COMPONENT.getKey(), "vertx")
        .withTag(Tags.HTTP_METHOD.getKey(), request.method().name())
        .withTag(Tags.HTTP_URL.getKey(), request.absoluteURI())
        .startActive(true);
    tracer.inject(scope.span().context(), Builtin.HTTP_HEADERS, new MultiMapInjectAdapter(request.headers()));
    return scope;
  }

  @Override
  public void requestEnd(Scope scope) {

  }

  @Override
  public void responseBegin(Scope scope, HttpClientResponse response) {

  }

  @Override
  public Scope responsePushed(Void endpointMetric, String socketMetric,
      SocketAddress localAddress, SocketAddress remoteAddress, HttpClientRequest request) {
    return null;
  }

  @Override
  public void requestReset(Scope scope) {

  }

  @Override
  public void responseEnd(Scope scope, HttpClientResponse response) {
    // Note that scope here is different to the scope created in #requestBegin because
    // instrumentation creates a new scope (with auto finish off)
    Tags.HTTP_STATUS.set(scope.span(), response.statusCode());
    scope.close();
  }

  @Override
  public String connected(Void endpointMetric, String socketMetric, WebSocket webSocket) {
    return null;
  }

  @Override
  public void disconnected(String webSocketMetric) {

  }

  @Override
  public String connected(SocketAddress remoteAddress, String remoteName) {
    return null;
  }

  @Override
  public void disconnected(String socketMetric, SocketAddress remoteAddress) {

  }

  @Override
  public void bytesRead(String socketMetric, SocketAddress remoteAddress, long numberOfBytes) {

  }

  @Override
  public void bytesWritten(String socketMetric, SocketAddress remoteAddress, long numberOfBytes) {

  }

  @Override
  public void exceptionOccurred(String socketMetric, SocketAddress remoteAddress, Throwable t) {

  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void close() {
  }
}
