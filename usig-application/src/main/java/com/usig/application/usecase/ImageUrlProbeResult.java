package com.usig.application.usecase;

record ImageUrlProbeResult(
    int statusCode, String contentType, long contentLengthBytes, byte[] contentPrefix) {

  ImageUrlProbeResult(int statusCode, String contentType, long contentLengthBytes) {
    this(statusCode, contentType, contentLengthBytes, new byte[0]);
  }

  ImageUrlProbeResult {
    contentPrefix = contentPrefix == null ? new byte[0] : contentPrefix.clone();
  }

  @Override
  public byte[] contentPrefix() {
    return contentPrefix.clone();
  }
}
