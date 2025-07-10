// Copyright (c) 2025 Skyflow, Inc.

package skyflow;

import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import skyflow.core.ClientOptions;
import skyflow.resources.service.AsyncServiceClient;

public class AsyncSkyflow {
  protected final ClientOptions clientOptions;

  private final AsyncRawSkyflow rawClient;

  protected final Supplier<AsyncServiceClient> serviceClient;

  public AsyncSkyflow(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    this.rawClient = new AsyncRawSkyflow(clientOptions);
    this.serviceClient = Suppliers.memoize(() -> new AsyncServiceClient(clientOptions));
  }

  /** Get responses with HTTP metadata like headers */
  public AsyncRawSkyflow withRawResponse() {
    return this.rawClient;
  }

  public AsyncServiceClient service() {
    return this.serviceClient.get();
  }

  public static AsyncSkyflowBuilder builder() {
    return new AsyncSkyflowBuilder();
  }
}
