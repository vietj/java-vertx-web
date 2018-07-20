package io.opentracing.contrib.vertx.core;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.HttpServerMetrics;

/**
 * @author Pavol Loffay
 */
public class OpenTracingServerMetrics implements HttpServerMetrics<Scope, Void, Void> {

  private final Tracer tracer;

  public OpenTracingServerMetrics(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public Scope requestBegin(Void socketMetric, HttpServerRequest request) {
    Scope scope = tracer.buildSpan(request.method().name())
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
        .withTag(Tags.COMPONENT.getKey(), "vertx")
        .withTag(Tags.HTTP_METHOD.getKey(), request.method().name())
        .withTag(Tags.HTTP_URL.getKey(), request.absoluteURI())
        .startActive(true);
    return scope;
  }

  @Override
  public void requestReset(Scope scope) {
  }

  @Override
  public Scope responsePushed(Void socketMetric, HttpMethod method, String uri,
      HttpServerResponse response) {
    return null;
  }

  @Override
  public void responseEnd(Scope scope, HttpServerResponse response) {
    Tags.HTTP_STATUS.set(scope.span(), response.getStatusCode());
    scope.close();
  }

  @Override
  public Void upgrade(Scope scope, ServerWebSocket serverWebSocket) {
    return null;
  }

  @Override
  public Void connected(Void socketMetric, ServerWebSocket serverWebSocket) {
    return null;
  }

  @Override
  public void disconnected(Void serverWebSocketMetric) {
  }

  @Override
  public Void connected(SocketAddress remoteAddress, String remoteName) {
    return null;
  }

  @Override
  public void disconnected(Void socketMetric, SocketAddress remoteAddress) {

  }

  @Override
  public void bytesRead(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
  }

  @Override
  public void bytesWritten(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
  }

  @Override
  public void exceptionOccurred(Void socketMetric, SocketAddress remoteAddress, Throwable t) {
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public void close() {

  }
}
