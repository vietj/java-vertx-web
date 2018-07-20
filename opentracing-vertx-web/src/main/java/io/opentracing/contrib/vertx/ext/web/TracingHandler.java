package io.opentracing.contrib.vertx.ext.web;

import io.opentracing.Scope;
import io.opentracing.contrib.vertx.ext.web.WebSpanDecorator.StandardTags;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

/**
 * Handler which creates tracing data for all server requests. It should be added to
 * {@link io.vertx.ext.web.Route#handler(Handler)} and {@link io.vertx.ext.web.Route#failureHandler(Handler)} as the
 * first in the chain.
 *
 * @author Pavol Loffay
 */
public class TracingHandler implements Handler<RoutingContext> {
    private static final Logger log = LoggerFactory.getLogger(TracingHandler.class);
    public static final String CURRENT_SPAN = TracingHandler.class.getName() + ".severSpan";

    private final Tracer tracer;
    private final List<WebSpanDecorator> decorators;

    public TracingHandler(Tracer tracer) {
        this(tracer, Collections.singletonList(new StandardTags()));
    }

    public TracingHandler(Tracer tracer, List<WebSpanDecorator> decorators) {
        this.tracer = tracer;
        this.decorators = new ArrayList<>(decorators);
    }

    @Override
    public void handle(RoutingContext routingContext) {
        if (routingContext.failed()) {
            handlerFailure(routingContext);
        } else {
            handlerNormal(routingContext);
        }
    }

    protected void handlerNormal(RoutingContext routingContext) {
        // reroute
        Object object = routingContext.get(CURRENT_SPAN);
        if (object instanceof Scope) {
            Scope scope = (Scope) object;
            decorators.forEach(spanDecorator ->
                    spanDecorator.onReroute(routingContext.request(), scope.span()));

            // TODO in 3.3.3 it was sufficient to add this when creating the span
            routingContext.addBodyEndHandler(finishEndHandler(routingContext, scope));
            routingContext.next();
            return;
        }

        SpanContext extractedContext = tracer.extract(Format.Builtin.HTTP_HEADERS,
                new MultiMapExtractAdapter(routingContext.request().headers()));

        Scope scope = tracer.buildSpan(routingContext.request().method().toString())
                .asChildOf(extractedContext)
                .ignoreActiveSpan() // important since we are on event loop
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                .startActive(false);

        decorators.forEach(spanDecorator ->
                spanDecorator.onRequest(routingContext.request(), scope.span()));

        routingContext.put(CURRENT_SPAN, scope);
        // TODO it's not guaranteed that body end handler is always called
        // https://github.com/vert-x3/vertx-web/issues/662
        routingContext.addBodyEndHandler(finishEndHandler(routingContext, scope));
        routingContext.next();
    }

    protected void handlerFailure(RoutingContext routingContext) {
        Object object = routingContext.get(CURRENT_SPAN);
        if (object instanceof Scope) {
            final Scope scope = (Scope)object;
            routingContext.addBodyEndHandler(event -> decorators.forEach(spanDecorator ->
                    spanDecorator.onFailure(routingContext.failure(), routingContext.response(), scope.span())));
        }

        routingContext.next();
    }

    private Handler<Void> finishEndHandler(RoutingContext routingContext, Scope scope) {
        return handler -> {
            decorators.forEach(spanDecorator ->
                    spanDecorator.onResponse(routingContext.request(), scope.span()));
            tracer.activeSpan().finish();
        };
    }

    /**
     * Helper method for accessing server span context associated with current request.
     *
     * @param routingContext routing context
     * @return server span context or null if not present
     */
    public static SpanContext serverSpanContext(RoutingContext routingContext) {
        SpanContext serverContext = null;

        Object object = routingContext.get(CURRENT_SPAN);
        if (object instanceof Scope) {
            Scope scope = (Scope) object;
            serverContext = scope.span().context();
        } else {
            log.error("Sever SpanContext is null or not an instance of SpanContext");
        }

        return serverContext;
    }
}
