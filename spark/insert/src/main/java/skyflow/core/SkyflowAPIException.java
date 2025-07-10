// Copyright (c) 2025 Skyflow, Inc.

package skyflow.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.Response;

public class SkyflowAPIException extends SkyflowException {
  /** The error code of the response that triggered the exception. */
  private final int statusCode;

  /** The body of the response that triggered the exception. */
  private final Object body;

  private final Map<String, List<String>> headers;

  public SkyflowAPIException(String message, int statusCode, Object body) {
    super(message);
    this.statusCode = statusCode;
    this.body = body;
    this.headers = new HashMap<>();
  }

  public SkyflowAPIException(String message, int statusCode, Object body, Response rawResponse) {
    super(message);
    this.statusCode = statusCode;
    this.body = body;
    this.headers = new HashMap<>();
    rawResponse
        .headers()
        .forEach(
            header -> {
              String key = header.component1();
              String value = header.component2();
              this.headers.computeIfAbsent(key, _str -> new ArrayList<>()).add(value);
            });
  }

  /**
   * @return the statusCode
   */
  public int statusCode() {
    return this.statusCode;
  }

  /**
   * @return the body
   */
  public Object body() {
    return this.body;
  }

  /**
   * @return the headers
   */
  public Map<String, List<String>> headers() {
    return this.headers;
  }

  @Override
  public String toString() {
    return "ApiClientApiException{"
        + "message: "
        + getMessage()
        + ", statusCode: "
        + statusCode
        + ", body: "
        + body
        + "}";
  }
}
