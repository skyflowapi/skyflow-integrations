// Copyright (c) 2025 Skyflow, Inc.

package skyflow;

import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import skyflow.core.ClientOptions;
import skyflow.resources.service.ServiceClient;

public class Skyflow {
  protected final ClientOptions clientOptions;

  private final RawSkyflow rawClient;

  protected final Supplier<ServiceClient> serviceClient;

  public Skyflow(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    this.rawClient = new RawSkyflow(clientOptions);
    this.serviceClient = Suppliers.memoize(() -> new ServiceClient(clientOptions));
  }

  /** Get responses with HTTP metadata like headers */
  public RawSkyflow withRawResponse() {
    return this.rawClient;
  }

  public ServiceClient service() {
    return this.serviceClient.get();
  }

  public static SkyflowBuilder builder() {
    return new SkyflowBuilder();
  }
}
