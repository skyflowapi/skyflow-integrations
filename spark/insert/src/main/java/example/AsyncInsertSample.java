// Copyright (c) 2025 Skyflow, Inc.

package example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import skyflow.AsyncSkyflow;
import skyflow.resources.service.requests.V1InsertRequest;
import skyflow.types.V1InsertRecordData;

public class AsyncInsertSample {
  public static void main(String[] args) {
    // Initialize HTTP client with Bearer token authentication
    OkHttpClient authClient =
        new OkHttpClient.Builder()
            .addInterceptor(
                chain -> {
                  Request original = chain.request();
                  Request requestWithAuth =
                      original.newBuilder().header("Authorization", "Bearer " + "TOKEN").build();
                  return chain.proceed(requestWithAuth);
                })
            .build();

    // Create Skyflow API client with vault URL and auth client
    AsyncSkyflow client = AsyncSkyflow.builder().url("VAULT_URL").httpClient(authClient).build();

    // Prepare data for insertion
    List<V1InsertRecordData> list = new ArrayList<>();
    Map<String, Object> map1 = new HashMap<>();

    // Add record fields to be inserted
    map1.put("name", "amit");
    map1.put("email", "amit@example.com");
    map1.put("dateOfBirth", "1990-01-01");
    map1.put("adult", true);
    map1.put("height", 170);

    Map<String, Object> map2 = new HashMap<>();

    // Add record fields to be inserted
    map2.put("name", "sai");
    map2.put("email", "sai@example.com");
    map2.put("dateOfBirth", "1990-01-01");
    map2.put("adult", true);
    map2.put("height", 180);

    // Create record data object and add to list
    V1InsertRecordData data1 = V1InsertRecordData.builder().data(map1).build();
    list.add(data1);

    V1InsertRecordData data2 = V1InsertRecordData.builder().data(map2).build();
    list.add(data2);

    // Build insert request with vault ID, table name and records
    V1InsertRequest req =
        V1InsertRequest.builder().vaultId("VAULT_ID").tableName("TABLE_NAME").records(list).build();

    // Execute insert operation and print response
    client
        .service()
        .insert(req)
        .thenAccept(
            response -> {
              System.out.println("Insert response: " + response);
            })
        .exceptionally(
            ex -> {
              System.err.println("Error during insert: " + ex.getMessage());
              return null;
            });
  }
}
