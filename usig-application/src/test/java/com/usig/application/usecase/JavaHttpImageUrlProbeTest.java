package com.usig.application.usecase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JavaHttpImageUrlProbeTest {

  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void sendsHeadThenRangeGetForContentSniffing() throws Exception {
    byte[] prefix = new byte[] {(byte) 0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a};
    List<String> methods = new ArrayList<>();
    AtomicReference<String> range = new AtomicReference<>();
    server =
        server(
            exchange -> {
              methods.add(exchange.getRequestMethod());
              if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("Content-Type", "image/png");
                exchange.getResponseHeaders().add("Content-Length", "1024");
                exchange.sendResponseHeaders(200, -1);
              } else {
                range.set(exchange.getRequestHeaders().getFirst("Range"));
                exchange.getResponseHeaders().add("Content-Type", "image/png");
                exchange.sendResponseHeaders(206, prefix.length);
                try (OutputStream body = exchange.getResponseBody()) {
                  body.write(prefix);
                }
              }
            });

    ImageUrlProbeResult result = probe().probe(uri("/image.png"));

    assertEquals(List.of("HEAD", "GET"), methods);
    assertEquals("bytes=0-15", range.get());
    assertEquals(200, result.statusCode());
    assertEquals("image/png", result.contentType());
    assertEquals(1024, result.contentLengthBytes());
    assertArrayEquals(prefix, result.contentPrefix());
  }

  @Test
  void doesNotFetchRangeWhenHeadRedirects() throws Exception {
    List<String> methods = new ArrayList<>();
    server =
        server(
            exchange -> {
              methods.add(exchange.getRequestMethod());
              exchange.getResponseHeaders().add("Location", "https://cdn.example.com/next.png");
              exchange.sendResponseHeaders(302, -1);
            });

    ImageUrlProbeResult result = probe().probe(uri("/redirect.png"));

    assertEquals(List.of("HEAD"), methods);
    assertEquals(302, result.statusCode());
    assertEquals(0, result.contentPrefix().length);
  }

  @Test
  void fallsBackToRangeWhenHeadIsNotAllowed() throws Exception {
    byte[] prefix = new byte[] {(byte) 0xff, (byte) 0xd8, (byte) 0xff};
    List<String> methods = new ArrayList<>();
    AtomicReference<String> range = new AtomicReference<>();
    server =
        server(
            exchange -> {
              methods.add(exchange.getRequestMethod());
              if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
              } else {
                range.set(exchange.getRequestHeaders().getFirst("Range"));
                exchange.getResponseHeaders().add("Content-Type", "image/jpeg");
                exchange.getResponseHeaders().add("Content-Range", "bytes 0-15/2048");
                exchange.sendResponseHeaders(206, prefix.length);
                try (OutputStream body = exchange.getResponseBody()) {
                  body.write(prefix);
                }
              }
            });

    ImageUrlProbeResult result = probe().probe(uri("/image.jpg"));

    assertEquals(List.of("HEAD", "GET"), methods);
    assertEquals("bytes=0-15", range.get());
    assertEquals(206, result.statusCode());
    assertEquals("image/jpeg", result.contentType());
    assertEquals(2048, result.contentLengthBytes());
    assertArrayEquals(prefix, result.contentPrefix());
  }

  @Test
  void fallsBackToRangeWhenHeadOmitsContentLength() throws Exception {
    byte[] prefix = new byte[] {(byte) 0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a};
    List<String> methods = new ArrayList<>();
    server =
        server(
            exchange -> {
              methods.add(exchange.getRequestMethod());
              if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("Content-Type", "image/png");
                exchange.sendResponseHeaders(200, -1);
              } else {
                exchange.getResponseHeaders().add("Content-Type", "image/png");
                exchange.getResponseHeaders().add("Content-Range", "bytes 0-15/4096");
                exchange.sendResponseHeaders(206, prefix.length);
                try (OutputStream body = exchange.getResponseBody()) {
                  body.write(prefix);
                }
              }
            });

    ImageUrlProbeResult result = probe().probe(uri("/image-without-length.png"));

    assertEquals(List.of("HEAD", "GET"), methods);
    assertEquals(206, result.statusCode());
    assertEquals("image/png", result.contentType());
    assertEquals(4096, result.contentLengthBytes());
    assertArrayEquals(prefix, result.contentPrefix());
  }

  private JavaHttpImageUrlProbe probe() {
    return new JavaHttpImageUrlProbe(
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(1))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build());
  }

  private URI uri(String path) {
    return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
  }

  private static HttpServer server(ThrowingHandler handler) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            handler.handle(exchange);
          } finally {
            exchange.close();
          }
        });
    server.setExecutor(Executors.newSingleThreadExecutor());
    server.start();
    return server;
  }

  @FunctionalInterface
  private interface ThrowingHandler {

    void handle(HttpExchange exchange) throws IOException;
  }
}
