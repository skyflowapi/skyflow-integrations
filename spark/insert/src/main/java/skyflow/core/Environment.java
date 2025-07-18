// Copyright (c) 2025 Skyflow, Inc.

package skyflow.core;

public final class Environment {
  private final String url;

  private Environment(String url) {
    this.url = url;
  }

  public String getUrl() {
    return this.url;
  }

  public static Environment custom(String url) {
    return new Environment(url);
  }
}
