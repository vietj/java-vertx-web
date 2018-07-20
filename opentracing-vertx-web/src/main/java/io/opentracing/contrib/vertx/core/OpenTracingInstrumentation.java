package io.opentracing.contrib.vertx.core;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.vertx.core.Handler;
import io.vertx.core.spi.instrumentation.Instrumentation;

/**
 * @author Pavol Loffay
 */
public class OpenTracingInstrumentation implements Instrumentation {

  private Tracer tracer;

  // service loader needs no-arg constructor
  public OpenTracingInstrumentation() {
    this(GlobalTracer.get());
  }

  public OpenTracingInstrumentation(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public <T> Handler<T> captureContinuation(Handler<T> handler) {
    return new TracingHandler<>(handler);
  }

  @Override
  public <T> Handler<T> unwrapContinuation(Handler<T> wrapper) {
    if (wrapper instanceof TracingHandler) {
      return ((TracingHandler) wrapper).getWrapped();
    }
    return wrapper;
  }

  public class TracingHandler<T> implements Handler<T>{
    private final Handler<T> wrapped;
    private final Span span;

    public TracingHandler(Handler<T> wrapped) {
      this.wrapped = wrapped;
      this.span = tracer.activeSpan();
    }

    @Override
    public void handle(T event) {
      Scope scope = null;
      if (span != null) {
        scope = tracer.scopeManager().activate(span, false);
      }
      try {
        wrapped.handle(event);
      } finally {
        if (scope != null) {
          scope.close();
        }
      }
    }

    public Handler<T> getWrapped() {
      return wrapped;
    }
  }
}
