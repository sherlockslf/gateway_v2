package com.usig.application.usecase;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.service.ServiceDescriptor;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class InputReferenceValidator {

  private static final int MAX_IMAGE_URL_LENGTH = 2048;
  private static final long MAX_IMAGE_BYTES = 10L * 1024 * 1024;
  private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES =
      Set.of("image/jpeg", "image/png", "image/webp", "image/gif");
  private final ImageHostResolver hostResolver;
  private final ImageUrlProbe urlProbe;

  InputReferenceValidator() {
    this(host -> Arrays.asList(InetAddress.getAllByName(host)), new JavaHttpImageUrlProbe());
  }

  InputReferenceValidator(ImageHostResolver hostResolver, ImageUrlProbe urlProbe) {
    this.hostResolver = hostResolver;
    this.urlProbe = urlProbe;
  }

  void validate(CreateRunCommand command, ServiceDescriptor service) {
    for (InputPart part : command.input()) {
      if (part.type() == InputPartType.IMAGE_REF) {
        validateImage(part, service);
      }
    }
  }

  boolean containsImage(CreateRunCommand command) {
    return command.input().stream().anyMatch(part -> part.type() == InputPartType.IMAGE_REF);
  }

  private void validateImage(InputPart part, ServiceDescriptor service) {
    if (part.value().length() > MAX_IMAGE_URL_LENGTH) {
      throw invalid("image URL exceeds maximum length");
    }
    URI uri = parse(part.value());
    if (!"https".equalsIgnoreCase(uri.getScheme())) {
      throw invalid("image URL must use https");
    }
    if (uri.getHost() == null || uri.getHost().isBlank()) {
      throw invalid("image URL must include host");
    }
    if (uri.getUserInfo() != null) {
      throw invalid("image URL must not include user info");
    }
    if (uri.getFragment() != null) {
      throw invalid("image URL must not include fragment");
    }
    if (isLocalOrIpLiteral(uri.getHost())) {
      throw invalid("image URL host is not allowed");
    }
    validateAllowedHost(uri.getHost(), service);
    String declaredMimeType = normalizedMimeType(part.mimeType());
    if (!SUPPORTED_IMAGE_MIME_TYPES.contains(declaredMimeType)) {
      throw invalid("image MIME type is not allowed");
    }
    validateResolvedHost(uri.getHost());
    validateRemoteMetadata(urlProbe.probe(uri), declaredMimeType);
  }

  private static void validateAllowedHost(String host, ServiceDescriptor service) {
    Set<String> allowedHosts = service.allowedImageHosts();
    if (allowedHosts.isEmpty()) {
      return;
    }
    String normalizedHost = host.toLowerCase(Locale.ROOT);
    boolean allowed =
        allowedHosts.stream()
            .anyMatch(
                allowedHost ->
                    normalizedHost.equals(allowedHost)
                        || normalizedHost.endsWith("." + allowedHost));
    if (!allowed) {
      throw invalid("image URL host is not in service allowlist");
    }
  }

  private void validateResolvedHost(String host) {
    List<InetAddress> addresses;
    try {
      addresses = hostResolver.resolve(host);
    } catch (UnknownHostException exception) {
      throw invalid("image URL host cannot be resolved");
    }
    if (addresses == null || addresses.isEmpty()) {
      throw invalid("image URL host cannot be resolved");
    }
    if (addresses.stream().anyMatch(InputReferenceValidator::isForbiddenAddress)) {
      throw invalid("image URL host resolves to a private address");
    }
  }

  private static boolean isForbiddenAddress(InetAddress address) {
    if (address.isAnyLocalAddress()
        || address.isLoopbackAddress()
        || address.isLinkLocalAddress()
        || address.isSiteLocalAddress()
        || address.isMulticastAddress()) {
      return true;
    }
    byte[] bytes = address.getAddress();
    if (bytes.length == 4) {
      return isForbiddenIpv4(bytes);
    }
    if (bytes.length == 16) {
      return isForbiddenIpv6(bytes);
    }
    return true;
  }

  private static boolean isForbiddenIpv4(byte[] bytes) {
    int first = Byte.toUnsignedInt(bytes[0]);
    int second = Byte.toUnsignedInt(bytes[1]);
    return first == 0
        || first == 10
        || first == 127
        || (first == 100 && second >= 64 && second <= 127)
        || (first == 169 && second == 254)
        || (first == 172 && second >= 16 && second <= 31)
        || (first == 192 && second == 168)
        || first >= 224;
  }

  private static boolean isForbiddenIpv6(byte[] bytes) {
    int first = Byte.toUnsignedInt(bytes[0]);
    return Arrays.equals(bytes, new byte[16]) || first == 0xfc || first == 0xfd;
  }

  private static void validateRemoteMetadata(ImageUrlProbeResult result, String declaredMimeType) {
    if (result == null) {
      throw invalid("image URL probe failed");
    }
    int statusCode = result.statusCode();
    if (statusCode >= 300 && statusCode < 400) {
      throw invalid("image URL redirects are not allowed");
    }
    if (statusCode < 200 || statusCode >= 300) {
      throw invalid("image URL probe returned non-success status");
    }
    if (result.contentLengthBytes() < 0) {
      throw invalid("image content length is required");
    }
    if (result.contentLengthBytes() > MAX_IMAGE_BYTES) {
      throw invalid("image content length exceeds maximum");
    }
    String remoteMimeType = normalizedMimeType(result.contentType());
    if (!SUPPORTED_IMAGE_MIME_TYPES.contains(remoteMimeType)
        || !remoteMimeType.equals(declaredMimeType)) {
      throw invalid("image content type does not match declared MIME type");
    }
    validateContentSignature(result.contentPrefix(), declaredMimeType);
  }

  private static void validateContentSignature(byte[] contentPrefix, String declaredMimeType) {
    if (contentPrefix.length == 0) {
      throw invalid("image content prefix is required");
    }
    boolean matches =
        switch (declaredMimeType) {
          case "image/jpeg" -> startsWith(contentPrefix, 0xff, 0xd8, 0xff);
          case "image/png" ->
              startsWith(contentPrefix, 0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a);
          case "image/gif" ->
              startsWith(contentPrefix, 'G', 'I', 'F', '8', '7', 'a')
                  || startsWith(contentPrefix, 'G', 'I', 'F', '8', '9', 'a');
          case "image/webp" ->
              contentPrefix.length >= 12
                  && startsWith(contentPrefix, 'R', 'I', 'F', 'F')
                  && contentPrefix[8] == (byte) 'W'
                  && contentPrefix[9] == (byte) 'E'
                  && contentPrefix[10] == (byte) 'B'
                  && contentPrefix[11] == (byte) 'P';
          default -> false;
        };
    if (!matches) {
      throw invalid("image content signature does not match declared MIME type");
    }
  }

  private static boolean startsWith(byte[] actual, int... expected) {
    if (actual.length < expected.length) {
      return false;
    }
    for (int index = 0; index < expected.length; index++) {
      if (actual[index] != (byte) expected[index]) {
        return false;
      }
    }
    return true;
  }

  private static String normalizedMimeType(String value) {
    if (value == null) {
      return "";
    }
    int parameterIndex = value.indexOf(';');
    String mimeType = parameterIndex >= 0 ? value.substring(0, parameterIndex) : value;
    return mimeType.trim().toLowerCase(Locale.ROOT);
  }

  private static URI parse(String value) {
    try {
      return new URI(value);
    } catch (URISyntaxException exception) {
      throw invalid("image URL is invalid");
    }
  }

  private static boolean isLocalOrIpLiteral(String host) {
    String normalized = host.toLowerCase(Locale.ROOT);
    if ("localhost".equals(normalized) || normalized.endsWith(".localhost")) {
      return true;
    }
    if (normalized.startsWith("[") && normalized.endsWith("]")) {
      return true;
    }
    return normalized.matches("\\d+(\\.\\d+){3}");
  }

  private static UseCaseException invalid(String message) {
    return new UseCaseException(ErrorCode.INVALID_REQUEST, message);
  }
}
