package io.opentracing.contrib.vertx.ext.web;

import io.opentracing.Scope;
import io.opentracing.mock.MockSpan;
import io.opentracing.tag.Tags;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.awaitility.Awaitility;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

/**
 * @author Pavol Loffay
 */
public class ClientTests extends BaseTracingTest {

  @Test
  public void testActiveSpanInHandler() {
    server.close();
    server.requestHandler(request -> {
      assertServerSpanActive(request);
      request.response()
          .setChunked(true)
          .write("hello\n")
          .end();
    });

    startServer(server);

    Scope scope = mockTracer.buildSpan("parent")
        .startActive(false);
    HttpClientRequest req = client.request(HttpMethod.GET, 8080, "localhost", "/hello", resp -> {
      assertEquals(200, resp.statusCode());
      MockSpan activeSpan = (MockSpan) mockTracer.activeSpan();
      assertEquals("parent", activeSpan.operationName());

      // TODO find out where the client span is being propagated
      // probably the active span should be available only in this callback or is there any other callback?

      // the server span is finished at this point
      List<MockSpan> mockSpans = mockTracer.finishedSpans();
      assertEquals(1, mockSpans.size());
      assertEquals(Tags.SPAN_KIND_SERVER, mockSpans.get(0).tags().get(Tags.SPAN_KIND.getKey()));
    });
    req.end();

    assertEquals(scope.span(), mockTracer.activeSpan());
    Awaitility.await().until(reportedSpansSize(), IsEqual.equalTo(2));
    scope.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    // 1. client span, 2. server spam
    assertEquals(2, mockSpans.size());
    assertOneTrace(mockSpans);
    MockSpan serverSpan = mockSpans.get(0);
    MockSpan clientSpan = mockSpans.get(1);
    assertEquals(Tags.SPAN_KIND_CLIENT, clientSpan.tags().get(Tags.SPAN_KIND.getKey()));
    assertEquals(clientSpan.context().spanId(), serverSpan.parentId());
  }
}
