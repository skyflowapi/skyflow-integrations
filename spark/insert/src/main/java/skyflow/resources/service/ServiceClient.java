// Copyright (c) 2025 Skyflow, Inc.

package skyflow.resources.service;

import skyflow.core.ClientOptions;
import skyflow.core.RequestOptions;
import skyflow.resources.service.requests.V1InsertRequest;
import skyflow.types.V1InsertResponse;

public class ServiceClient {
  protected final ClientOptions clientOptions;

  private final RawServiceClient rawClient;

  public ServiceClient(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    this.rawClient = new RawServiceClient(clientOptions);
  }

  /** Get responses with HTTP metadata like headers */
  public RawServiceClient withRawResponse() {
    return this.rawClient;
  }

  public V1InsertResponse insert() {
    return this.rawClient.insert().body();
  }

  public V1InsertResponse insert(V1InsertRequest request) {
    return this.rawClient.insert(request).body();
  }

  public V1InsertResponse insert(V1InsertRequest request, RequestOptions requestOptions) {
    return this.rawClient.insert(request, requestOptions).body();
  }
}
