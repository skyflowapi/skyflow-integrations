// Copyright (c) 2025 Skyflow, Inc.

package skyflow.resources.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import skyflow.core.ClientOptions;
import skyflow.core.MediaTypes;
import skyflow.core.ObjectMappers;
import skyflow.core.RequestOptions;
import skyflow.core.SkyflowAPIException;
import skyflow.core.SkyflowException;
import skyflow.core.SkyflowHttpResponse;
import skyflow.resources.service.requests.V1InsertRequest;
import skyflow.types.V1InsertResponse;

public class RawServiceClient {
  protected final ClientOptions clientOptions;

  public RawServiceClient(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
  }

  public SkyflowHttpResponse<V1InsertResponse> insert() {
    return insert(V1InsertRequest.builder().build());
  }

  public SkyflowHttpResponse<V1InsertResponse> insert(V1InsertRequest request) {
    return insert(request, null);
  }

  public SkyflowHttpResponse<V1InsertResponse> insert(
      V1InsertRequest request, RequestOptions requestOptions) {
    HttpUrl httpUrl =
        HttpUrl.parse(this.clientOptions.environment().getUrl())
            .newBuilder()
            .addPathSegments("v2/records/insert")
            .build();
    RequestBody body;
    try {
      body =
          RequestBody.create(
              MediaTypes.APPLICATION_JSON, ObjectMappers.JSON_MAPPER.writeValueAsBytes(request));
    } catch (JsonProcessingException e) {
      throw new SkyflowException("Failed to serialize request", e);
    }
    Request okhttpRequest =
        new Request.Builder()
            .url(httpUrl)
            .method("POST", body)
            .headers(Headers.of(clientOptions.headers(requestOptions)))
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .build();
    OkHttpClient client = clientOptions.httpClient();
    if (requestOptions != null && requestOptions.getTimeout().isPresent()) {
      client = clientOptions.httpClientWithTimeout(requestOptions);
    }
    try (Response response = client.newCall(okhttpRequest).execute()) {
      ResponseBody responseBody = response.body();
      if (response.isSuccessful()) {
        return new SkyflowHttpResponse<>(
            ObjectMappers.JSON_MAPPER.readValue(responseBody.string(), V1InsertResponse.class),
            response);
      }
      String responseBodyString = responseBody != null ? responseBody.string() : "{}";
      throw new SkyflowAPIException(
          "Error with status code " + response.code(),
          response.code(),
          ObjectMappers.JSON_MAPPER.readValue(responseBodyString, Object.class),
          response);
    } catch (IOException e) {
      throw new SkyflowException("Network error executing HTTP request", e);
    }
  }
}
