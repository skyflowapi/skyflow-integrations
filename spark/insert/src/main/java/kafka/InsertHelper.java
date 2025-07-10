// Copyright (c) 2025 Skyflow, Inc.

package kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.skyflow.entities.ResponseToken;
import com.skyflow.errors.SkyflowException;
import com.skyflow.serviceaccount.util.Token;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyflow.AsyncSkyflow;
import skyflow.core.ObjectMappers;
import skyflow.resources.service.requests.V1InsertRequest;
import skyflow.types.V1InsertRecordData;
import skyflow.types.V1InsertResponse;

// This class implements Serializable so that it can be transmitted between Spark executors.
// The serialVersionUID is defined to maintain version consistency during deserialization.
public class InsertHelper implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertHelper.class);

  private final String vaultUrl;
  private final String vaultId;
  private final String vaultCredentials;
  private final String tableName;

  private transient AsyncSkyflow client;
  private transient String vaultToken; // this should also be transient to ensure fresh token

  public InsertHelper(String vaultUrl, String vaultId, String vaultCredentials, String tableName) {
    this.vaultUrl = vaultUrl;
    this.vaultId = vaultId;
    this.vaultCredentials = vaultCredentials;
    this.tableName = tableName;

    refreshVaultToken();
    initializeClient();
  }

  /** Refreshes the cached vault bearer token. */
  private void refreshVaultToken() {
    try {
      LOGGER.info("Generating new vault token...");
      ResponseToken tokenResponse = Token.generateBearerTokenFromCreds(vaultCredentials);
      this.vaultToken = tokenResponse.getAccessToken();
      LOGGER.info("Vault token generated successfully");
    } catch (SkyflowException e) {
      LOGGER.error("Failed to generate vault token", e);
      throw new RuntimeException("Could not authenticate to Skyflow: " + e);
    }
  }

  /** Builds the AsyncSkyflow client, injecting the Authorization header dynamically. */
  private void initializeClient() {
    LOGGER.info("Initializing Skyflow client (vaultUrl={})", vaultUrl);

    Interceptor authInterceptor =
        chain -> {
          Request reqWithAuth =
              chain.request().newBuilder().header("Authorization", "Bearer " + vaultToken).build();
          return chain.proceed(reqWithAuth);
        };

    OkHttpClient httpClient = new OkHttpClient.Builder().addInterceptor(authInterceptor).build();

    this.client = AsyncSkyflow.builder().url(vaultUrl).httpClient(httpClient).build();

    LOGGER.info("Skyflow client initialized");
  }

  /** Reinitialize transient fields after deserialization. */
  @SuppressWarnings("unused")
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject(); // Deserialize non-transient fields
    refreshVaultToken(); // regenerate token upon deserialization
    initializeClient(); // rebuild transient client
  }

  public CompletableFuture<V1InsertResponse> insertAsync(Iterable<Row> rows) {
    if (client == null || vaultToken == null || Token.isExpired(vaultToken)) {
      LOGGER.info("Client is null or Vault token expired, refreshing...");
      refreshVaultToken();
      initializeClient();
    }
    List<V1InsertRecordData> records = mapRowsToInsertData(rows);
    if (records.isEmpty()) {
      LOGGER.info("No records found in this batch; skipping insert");
      return CompletableFuture.completedFuture(null);
    }
    LOGGER.info(
        "Inserting {} record(s) into Skyflow (vaultId={}, table={})",
        records.size(),
        vaultId,
        tableName);
    V1InsertRequest request =
        V1InsertRequest.builder().vaultId(vaultId).tableName(tableName).records(records).build();
    LOGGER.info(
        "Inserting Request Built: {} record(s) into Skyflow (vaultId={}, table={})",
        records.size(),
        vaultId,
        tableName);
    CompletableFuture<V1InsertResponse> future = new CompletableFuture<>();
    client
        .service()
        .insert(request)
        .thenAccept(future::complete)
        .exceptionally(
            ex -> {
              try {
                LOGGER.error(
                    "Error during Skyflow insert {}",
                    ObjectMappers.JSON_MAPPER.writeValueAsString(ex.toString()));
              } catch (JsonProcessingException e) {
                LOGGER.error("Failed to serialize exception during Skyflow insert", e);
              }
              LOGGER.error("Closing the Skyflow insert");
              future.completeExceptionally(ex);
              return null;
            });
    return future;
  }

  /** Transforms Spark Rows into Skyflow insert payloads. */
  private List<V1InsertRecordData> mapRowsToInsertData(Iterable<Row> rows) {
    List<V1InsertRecordData> list = new ArrayList<>();
    for (Row wrapper : rows) {
      Row recordRow = wrapper.getAs("record");
      if (recordRow == null) {
        LOGGER.warn("Skipping null record in row: {}", wrapper);
        continue;
      }

      Map<String, Object> dataMap = new HashMap<>();
      for (String fieldName : recordRow.schema().fieldNames()) {
        Object value = recordRow.getAs(fieldName);
        dataMap.put(fieldName, value);
      }

      LOGGER.debug("Mapped record: {}", dataMap);
      list.add(V1InsertRecordData.builder().data(dataMap).build());
    }
    return list;
  }
}
