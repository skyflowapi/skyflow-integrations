// Copyright (c) 2025 Skyflow, Inc.

package skyflow.core;

public class SkyflowException extends RuntimeException {
  public SkyflowException(String message) {
    super(message);
  }

  public SkyflowException(String message, Exception e) {
    super(message, e);
  }
}
