// Copyright (c) 2025 Skyflow, Inc.

package skyflow.resources.service;

import java.util.concurrent.CompletableFuture;
import skyflow.core.ClientOptions;
import skyflow.core.RequestOptions;
import skyflow.resources.service.requests.V1InsertRequest;
import skyflow.types.V1InsertResponse;

public class AsyncServiceClient {
  protected final ClientOptions clientOptions;

  private final AsyncRawServiceClient rawClient;

  public AsyncServiceClient(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    this.rawClient = new AsyncRawServiceClient(clientOptions);
  }

  /** Get responses with HTTP metadata like headers */
  public AsyncRawServiceClient withRawResponse() {
    return this.rawClient;
  }

  public CompletableFuture<V1InsertResponse> insert() {
    return this.rawClient.insert().thenApply(response -> response.body());
  }

  public CompletableFuture<V1InsertResponse> insert(V1InsertRequest request) {
    return this.rawClient.insert(request).thenApply(response -> response.body());
  }

  public CompletableFuture<V1InsertResponse> insert(
      V1InsertRequest request, RequestOptions requestOptions) {
    return this.rawClient.insert(request, requestOptions).thenApply(response -> response.body());
  }
}
