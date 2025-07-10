// Copyright (c) 2025 Skyflow, Inc.

package skyflow;

import okhttp3.OkHttpClient;
import skyflow.core.ClientOptions;
import skyflow.core.Environment;

public class AsyncSkyflowBuilder {
  private ClientOptions.Builder clientOptionsBuilder = ClientOptions.builder();

  private Environment environment;

  public AsyncSkyflowBuilder url(String url) {
    this.environment = Environment.custom(url);
    return this;
  }

  /** Sets the timeout (in seconds) for the client. Defaults to 60 seconds. */
  public AsyncSkyflowBuilder timeout(int timeout) {
    this.clientOptionsBuilder.timeout(timeout);
    return this;
  }

  /** Sets the maximum number of retries for the client. Defaults to 2 retries. */
  public AsyncSkyflowBuilder maxRetries(int maxRetries) {
    this.clientOptionsBuilder.maxRetries(maxRetries);
    return this;
  }

  /** Sets the underlying OkHttp client */
  public AsyncSkyflowBuilder httpClient(OkHttpClient httpClient) {
    this.clientOptionsBuilder.httpClient(httpClient);
    return this;
  }

  public AsyncSkyflow build() {
    clientOptionsBuilder.environment(this.environment);
    return new AsyncSkyflow(clientOptionsBuilder.build());
  }
}
