package com.usig.application.usecase;

import com.usig.domain.error.ErrorCode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

final class JavaHttpImageUrlProbe implements ImageUrlProbe {

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);
  private static final String SNIFF_RANGE = "bytes=0-15";

  private final HttpClient httpClient;

  JavaHttpImageUrlProbe() {
    this(
        HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build());
  }

  JavaHttpImageUrlProbe(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  @Override
  public ImageUrlProbeResult probe(URI uri) {
    HttpResponse<Void> headResponse = sendHead(uri);
    String headContentType = headResponse.headers().firstValue("content-type").orElse("");
    long headContentLength = headResponse.headers().firstValueAsLong("content-length").orElse(-1L);
    int headStatus = headResponse.statusCode();
    if (headStatus >= 300 && headStatus < 400) {
      return new ImageUrlProbeResult(headStatus, headContentType, headContentLength);
    }
    if (headStatus >= 200 && headStatus < 300 && hasMetadata(headContentType, headContentLength)) {
      RangeResponse rangeResponse = sendRange(uri);
      return new ImageUrlProbeResult(
          headStatus, headContentType, headContentLength, rangeResponse.body());
    }
    if (isFallbackEligible(headStatus)) {
      RangeResponse rangeResponse = sendRange(uri);
      if (rangeResponse.statusCode() != 206) {
        throw invalid("image URL fallback range probe must return partial content");
      }
      String contentType =
          headContentType.isBlank() ? rangeResponse.contentType() : headContentType;
      long contentLength =
          headContentLength >= 0 ? headContentLength : parseCompleteLength(rangeResponse);
      return new ImageUrlProbeResult(
          rangeResponse.statusCode(), contentType, contentLength, rangeResponse.body());
    }
    return new ImageUrlProbeResult(headStatus, headContentType, headContentLength);
  }

  private HttpResponse<Void> sendHead(URI uri) {
    HttpRequest request =
        HttpRequest.newBuilder(uri)
            .timeout(REQUEST_TIMEOUT)
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .build();
    try {
      return httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    } catch (IOException exception) {
      throw invalid("image URL probe failed");
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw invalid("image URL probe interrupted");
    }
  }

  private RangeResponse sendRange(URI uri) {
    HttpRequest request =
        HttpRequest.newBuilder(uri)
            .timeout(REQUEST_TIMEOUT)
            .header("Range", SNIFF_RANGE)
            .GET()
            .build();
    HttpResponse<byte[]> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    } catch (IOException exception) {
      throw invalid("image URL content sniff failed");
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw invalid("image URL content sniff interrupted");
    }
    if (response.statusCode() != 200 && response.statusCode() != 206) {
      throw invalid("image URL content sniff returned non-success status");
    }
    return new RangeResponse(
        response.statusCode(),
        response.headers().firstValue("content-type").orElse(""),
        response.headers().firstValueAsLong("content-length").orElse(-1L),
        response.headers().firstValue("content-range").orElse(""),
        response.body());
  }

  private static boolean hasMetadata(String contentType, long contentLength) {
    return !contentType.isBlank() && contentLength >= 0;
  }

  private static boolean isFallbackEligible(int headStatus) {
    return headStatus >= 200 && headStatus < 300 || headStatus == 405 || headStatus == 501;
  }

  private static long parseCompleteLength(RangeResponse rangeResponse) {
    String contentRange = rangeResponse.contentRange();
    int slashIndex = contentRange.lastIndexOf('/');
    if (!contentRange.startsWith("bytes ")
        || slashIndex < 0
        || slashIndex == contentRange.length() - 1) {
      throw invalid("image URL fallback range probe must include content range length");
    }
    String completeLength = contentRange.substring(slashIndex + 1).trim();
    if ("*".equals(completeLength)) {
      throw invalid("image URL fallback range probe must include content range length");
    }
    try {
      return Long.parseLong(completeLength);
    } catch (NumberFormatException exception) {
      throw invalid("image URL fallback range probe has invalid content range length");
    }
  }

  private static UseCaseException invalid(String message) {
    return new UseCaseException(ErrorCode.INVALID_REQUEST, message);
  }

  private record RangeResponse(
      int statusCode,
      String contentType,
      long contentLengthBytes,
      String contentRange,
      byte[] body) {}
}
