package io.opentracing.contrib.vertx.ext.web;

import io.opentracing.contrib.vertx.core.OpenTracingInstrumentation;
import io.opentracing.contrib.vertx.core.OpenTracingVertxMetrics;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.opentracing.util.GlobalTracerTestUtil;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.spi.instrumentation.InstrumentationFactory;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.web.WebTestBase;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.junit.Before;

/**
 * @author Pavol Loffay
 */
public class BaseTracingTest extends WebTestBase {

  protected MockTracer mockTracer;
  protected HttpClient notInstrumentedClient;


  @Override
  public void setUp() throws Exception {
    mockTracer = new MockTracer();
    // just for debug GlobalTracer.get()..
    GlobalTracerTestUtil.resetGlobalTracer();
    GlobalTracer.register(mockTracer);
    super.setUp();
//    TracingHandler withStandardTags = new TracingHandler(mockTracer, Collections.singletonList(new StandardTags()));
//    router.route()
//        .order(-1).handler(withStandardTags)
//        .failureHandler(withStandardTags);

    InstrumentationFactory.setInstrumentation(new OpenTracingInstrumentation(mockTracer));
    notInstrumentedClient = Vertx.vertx().createHttpClient();
  }

  @Override
  protected VertxOptions getOptions() {
    //force one event loop to make testing active-span bugs easier
    return super.getOptions()
        .setEventLoopPoolSize(1)
        .setMetricsOptions(new MetricsOptions()
            .setEnabled(true)
            .setFactory(options -> vertxMetrics())
        );
  }

  protected VertxMetrics vertxMetrics() {
    return new OpenTracingVertxMetrics(mockTracer);
  }

  @Before
  public void beforeTest()  {
    mockTracer.reset();
  }

  protected void notTracedRequest(String path, HttpMethod method, int statusCode) {
    HttpClientRequest req = notInstrumentedClient.request(method, 8080, "localhost", path, resp -> {
      assertEquals(statusCode, resp.statusCode());
    });
    req.end();
  }

  protected void tracedRequest(String path, HttpMethod method, int statusCode) {
    HttpClientRequest req = client.request(method, 8080, "localhost", path, resp -> {
      assertEquals(statusCode, resp.statusCode());
    });
    req.end();
  }

  protected Callable<Integer> reportedSpansSize() {
    return () -> mockTracer.finishedSpans().size();
  }

  public void assertOneTrace(List<MockSpan> mockSpans) {
    for (int i = 1; i < mockSpans.size(); i++) {
      assertEquals(mockSpans.get(i - 1).context().traceId(), mockSpans.get(i).context().traceId());
    }
  }

  protected void assertServerSpanActive(HttpServerRequest request) {
    MockSpan span = (MockSpan) mockTracer.activeSpan();
    assertEquals(Tags.SPAN_KIND_SERVER, span.tags().get(Tags.SPAN_KIND.getKey()));
    assertTrue(span.tags().get(Tags.HTTP_URL.getKey()).toString().equals(request.absoluteURI()));
  }

  static void startServer(HttpServer server) {
    CountDownLatch latch = new CountDownLatch(1);
    server.listen(8080, event -> latch.countDown());
    latch.countDown();
  }
}
