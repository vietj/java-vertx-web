package io.opentracing.contrib.vertx.ext.web;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockSpan.MockContext;
import io.opentracing.tag.Tags;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.handler.TimeoutHandler;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.vertx.test.core.Repeat;
import org.awaitility.Awaitility;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Pavol Loffay
 */
public class TracingHandlerTest extends BaseTracingTest {

    protected HttpClientOptions getHttpClientOptions() {
        // Concurrent chaining test need more than 5 concurrent connections
        return super.getHttpClientOptions().setMaxPoolSize(10);
    }

    @Test
    public void testNoURLMapping() throws Exception {
        {
            notTracedRequest("/noUrlMapping", HttpMethod.GET, 404);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(5, mockSpan.tags().size());
        Assert.assertEquals(404, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/noUrlMapping", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testStandardTags() throws Exception {
        {
            router.route("/hello").handler(routingContext -> {
                assertServerSpanActive(routingContext.request());
                routingContext.response()
                        .setChunked(true)
                        .write("hello\n")
                        .end();
            });

            notTracedRequest("/hello", HttpMethod.GET, 200);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
//        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(5, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_SERVER, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertEquals("vertx", mockSpan.tags().get(Tags.COMPONENT.getKey()));
        Assert.assertEquals(200, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/hello", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testReroute() throws Exception {
        {
            router.route("/route1").handler(routingContext -> {
                assertServerSpanActive(routingContext.request());
                routingContext.reroute("/route2");
            });

            router.route("/route2").handler(routingContext -> {
                // TODO not passing
//                assertServerRequest(routingContext);
                routingContext.response()
                        .setStatusCode(205)
                        .setChunked(true)
                        .write("dsds")
                        .end();
            });

            notTracedRequest("/route1", HttpMethod.GET, 205);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(5, mockSpan.tags().size());
        Assert.assertEquals(205, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/route1", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(1, mockSpan.logEntries().size());
        Assert.assertEquals(3, mockSpan.logEntries().get(0).fields().size());
        Assert.assertEquals("reroute", mockSpan.logEntries().get(0).fields().get("event"));
        Assert.assertEquals("http://localhost:8080/route2",
                mockSpan.logEntries().get(0).fields().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals("GET",
            mockSpan.logEntries().get(0).fields().get(Tags.HTTP_METHOD.getKey()));
    }

    @Test
    public void testRerouteFailures() throws Exception {
        {
            router.route("/route1").handler(routingContext -> {
                assertServerSpanActive(routingContext.request());
                routingContext.reroute("/route2");
            }).failureHandler(event -> {
                event.response().setStatusCode(400);
            });

            router.route("/route2").handler(routingContext -> {
                throw new IllegalArgumentException("e");
            }).failureHandler(event -> {
                event.response().setStatusCode(401).end();
            });

            notTracedRequest("/route1", HttpMethod.GET, 401);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Boolean.TRUE, mockSpan.tags().get(Tags.ERROR.getKey()));
        Assert.assertEquals(401, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/route1", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(2, mockSpan.logEntries().size());
    }

    @Test
    public void testMultipleRoutes() throws Exception {
        {
            router.route("/route").handler(routingContext -> {
                assertServerSpanActive(routingContext.request());
                routingContext.response()
                        .setChunked(true)
                        .setStatusCode(205)
                        .write("route1");

                routingContext.next();
            });

            router.route("/route").handler(routingContext -> {
                assertServerSpanActive(routingContext.request());
                routingContext.response()
                        .write("route2")
                        .end();
            });

            notTracedRequest("/route", HttpMethod.GET, 205);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(5, mockSpan.tags().size());
        Assert.assertEquals(205, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/route", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testLocalSpan() throws Exception {
        {
            router.route("/localSpan").handler(routingContext -> {
                SpanContext serverSpanContext = TracingHandler.serverSpanContext(routingContext);
                io.opentracing.Tracer.SpanBuilder spanBuilder = mockTracer.buildSpan("localSpan");

                spanBuilder.asChildOf(serverSpanContext)
                        .startManual()
                        .finish();

                routingContext.response()
                        .setStatusCode(202)
                        .end();
            });

            notTracedRequest("/localSpan", HttpMethod.GET, 202);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(2));
        }
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(2, mockSpans.size());

        Assert.assertEquals(mockSpans.get(0).parentId(), mockSpans.get(1).context().spanId());
        Assert.assertEquals(mockSpans.get(0).context().traceId(), mockSpans.get(1).context().traceId());
    }

    @Test
    public void testFailRoutingContext() throws Exception {
        {
            router.route("/fail").handler(routingContext -> {
                routingContext.fail(501);
            });

            notTracedRequest("/fail", HttpMethod.GET, 501);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Boolean.TRUE, mockSpan.tags().get(Tags.ERROR.getKey()));
        Assert.assertEquals(501, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/fail", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testExceptionInHandler() throws Exception {
        {
            router.route("/exception").handler(routingContext -> {
                throw new IllegalArgumentException("msg");
            });

            notTracedRequest("/exception", HttpMethod.GET,500);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Boolean.TRUE, mockSpan.tags().get(Tags.ERROR.getKey()));
        Assert.assertEquals(500, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/exception", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(1, mockSpan.logEntries().size());
        Assert.assertEquals(2, mockSpan.logEntries().get(0).fields().size());
        Assert.assertEquals(Tags.ERROR.getKey(), mockSpan.logEntries().get(0).fields().get("event"));
        Assert.assertTrue(mockSpan.logEntries().get(0).fields().get("error.object") instanceof Throwable);
    }

    @Test
    public void testExceptionInHandlerWithFailureHandler() throws Exception {
        {
            router.route("/exceptionWithHandler").handler(routingContext -> {
                throw new IllegalArgumentException("msg");
            }).failureHandler(event -> {
                event.response()
                        .setStatusCode(404)
                        .end();
            });

            notTracedRequest("/exceptionWithHandler", HttpMethod.GET, 404);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Boolean.TRUE, mockSpan.tags().get(Tags.ERROR.getKey()));
        Assert.assertEquals(404, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/exceptionWithHandler", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(1, mockSpan.logEntries().size());
        Assert.assertEquals(2, mockSpan.logEntries().get(0).fields().size());
        Assert.assertEquals(Tags.ERROR.getKey(), mockSpan.logEntries().get(0).fields().get("event"));
        Assert.assertTrue(mockSpan.logEntries().get(0).fields().get("error.object") instanceof Throwable);
    }

    @Test
    public void testTimeoutHandler() throws Exception {
        {
            router.route().handler(TimeoutHandler.create(300, 501));

            router.route("/timeout")
                    .blockingHandler(routingContext -> {
                        try {
                            Thread.sleep(10000);
                            routingContext.response()
                                    .setStatusCode(202)
                                    .end();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            routingContext.response().end();
                        }
                    });

            notTracedRequest("/timeout", HttpMethod.GET, 501);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Boolean.TRUE, mockSpan.tags().get(Tags.ERROR.getKey()));
        Assert.assertEquals(501, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/timeout", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testBodyEndHandler() throws Exception {
        {
            router.route("/bodyEnd").handler(routingContext -> {
                    routingContext.addBodyEndHandler(event -> {
                        // noop
                    });

                    routingContext.response().end();
                });

            notTracedRequest("/bodyEnd", HttpMethod.GET, 200);
            Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(1));
        }
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(5, mockSpan.tags().size());
        Assert.assertEquals(200, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals("http://localhost:8080/bodyEnd", mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    /**
     * If someone incorrectly starts a span on an event loop, the TracingHandler was previously using it as the current
     * active span to be a child of. Such functionality is correct in a thread-per-request environment but not
     * in an event loop model. The tracinghandler now `ignoreActiveSpans` which is a better safeguard against the
     * problem.
     * 
     */
    @Test
    public void testIgnoringActiveSpan() throws Exception {
        final CountDownLatch firstLatch = new CountDownLatch(1);
        final CountDownLatch secondLatch = new CountDownLatch(1);

        router.route("/wait").handler(context -> {
            assertServerSpanActive(context.request());
            Scope scope = mockTracer.buildSpan("internal")
                    .startActive(true);
            vertx().executeBlocking((result) -> {
                firstLatch.countDown();
                try {
                    awaitLatch(secondLatch);
                } catch (InterruptedException e) {
                    result.fail(e);
                }
                result.complete();
            }, result -> {
                scope.close();
                context.response().end();
            });
        });

        //perform two requests -- we want to block
        //inside the handler and make an active span.
        notTracedRequest("/wait", HttpMethod.GET, 200);
        awaitLatch(firstLatch);

        notTracedRequest("/wait", HttpMethod.GET, 200);
        //they should both me in the router now -- resume the latch
        secondLatch.countDown();

        Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(2));
        for (MockSpan span : mockTracer.finishedSpans()) {
            Assert.assertEquals(span.parentId(), 0);
        }
    }

    @Test
    public void testMultiple() throws Exception {
        closeServer();
        server.requestHandler(request -> {
            assertServerSpanActive(request);
            request.response()
                .setChunked(true)
                .write("hello\n")
                .end();
        });

        startServer(server);
        int numberOfRequests = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < numberOfRequests; i++) {
            executorService.submit(() -> notTracedRequest("/hello", HttpMethod.GET, 200));
        }

        try {
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();

        Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(100));
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        assertEquals(numberOfRequests, mockSpans.size());
        Set<Long> traceIds = mockSpans.stream()
            .map(MockSpan::context)
            .map(MockContext::traceId)
            .collect(Collectors.toSet());
        assertEquals(numberOfRequests, traceIds.size());
    }

    @Test
    public void testChaining() throws Exception {
        closeServer();
        server.requestHandler(request -> {
            if (request.uri().contains("/hello")) {
                request.response()
                    .setChunked(true)
                    .write("hello\n")
                    .end();
            } else if (request.uri().contains("/chaining")) {
                HttpClientRequest req = client.get(8080, "localhost", "/hello", resp -> {
                    assertServerSpanActive(request);
                    request.response()
                        .setChunked(true)
                        .write("chaining\n")
                        .end();
                });
                req.end();
            }
        });

        startServer(server);
        notTracedRequest("/chaining", HttpMethod.GET, 200);

        // TODO it's failing on span.close() - the scope being closed is not the one currently active so it's baling out
        Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(3));

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        assertEquals(3, mockSpans.size());
        assertOneTrace(mockSpans);
    }

    private void closeServer() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        server.close(onSuccess(v -> latch.countDown()));
        awaitLatch(latch);
    }

    @Test
    public void testChainingConcurrent() throws Exception {
        closeServer();
        server.requestHandler(request -> {
            if (request.uri().contains("/hello")) {
                assertServerSpanActive(request);
                request.response()
                    .setChunked(true)
                    .write("hello\n")
                    .end();
            } else if (request.uri().contains("/chaining")) {
                assertServerSpanActive(request);
                AtomicInteger counter = new AtomicInteger(0);
                int count = Integer.parseInt(request.params().get("count"));
                for (int i = 0; i < count; i++) {
                    client.getNow(8080, "localhost", "/hello", resp -> {
                        if (counter.incrementAndGet() == count) {
                            request.response()
                                .setChunked(true)
                                .write("chaining\n")
                                .end();
                        }
                    });
                }
            }
        });

        startServer(server);
        int count = 6;
        notTracedRequest("/chaining?count=" + count, HttpMethod.GET, 200);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(reportedSpansSize(), IsEqual.equalTo(count * 2 + 1));
        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        assertEquals(1 + count*2,  mockSpans.size());

        assertOneTrace(mockSpans);

        Map<Long, MockSpan> spanMap = mockSpans.stream()
            .collect(Collectors.toMap(o -> o.context().spanId(), Function.identity()));
        List<MockSpan> serverHelloSpans = mockSpans.stream()
            .filter(mockSpan ->  mockSpan.tags().get(Tags.SPAN_KIND.getKey()).equals(Tags.SPAN_KIND_SERVER))
            .filter(mockSpan -> mockSpan.tags().get(Tags.HTTP_URL.getKey()).toString().contains("/hello"))
            .collect(Collectors.toList());

        for (MockSpan serverHelloSpan: serverHelloSpans) {
            MockSpan clientHelloSpan = spanMap.get(serverHelloSpan.parentId());
            assertEquals(Tags.SPAN_KIND_CLIENT, clientHelloSpan.tags().get(Tags.SPAN_KIND.getKey()));
            assertTrue(clientHelloSpan.tags().get(Tags.HTTP_URL.getKey()).toString().contains("/hello"));

            MockSpan serverChainingSpan = spanMap.get(clientHelloSpan.parentId());
            assertEquals(Tags.SPAN_KIND_SERVER, serverChainingSpan.tags().get(Tags.SPAN_KIND.getKey()));
            assertTrue(serverChainingSpan.tags().get(Tags.HTTP_URL.getKey()).toString().contains("/chaining"));
        }
    }
}
