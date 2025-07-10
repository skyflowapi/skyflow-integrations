// Copyright (c) 2025 Skyflow, Inc.

package util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecretManagerReader {

  // Logger instance for logging
  private static final Logger LOGGER = LoggerFactory.getLogger(SecretManagerReader.class);

  /**
   * Reads the credentials from Google Cloud Secret Manager.
   *
   * @param secretName Name of the secret (format:
   *     projects/{project-id}/secrets/{secret-name}/versions/{version-number})
   * @return Secret payload as a String
   */
  public static String readCredentials(String secretName) {
    LOGGER.info("Reading credentials from Secret Manager for secret: {}", secretName);

    // Initialize the Secret Manager Client
    try {
      SecretManagerServiceClient client = SecretManagerServiceClient.create();
      // Build the request to access the secret version
      AccessSecretVersionRequest request =
          AccessSecretVersionRequest.newBuilder().setName(secretName).build();

      // Access the secret version
      AccessSecretVersionResponse response = client.accessSecretVersion(request);

      // Retrieve secret payload
      String payload = response.getPayload().getData().toStringUtf8();
      LOGGER.info("Successfully read credentials from Secret Manager.");

      return payload;

    } catch (IOException e) {
      LOGGER.error("IOException occurred while accessing Secret Manager: ", e);
      throw new RuntimeException("Failed to read credentials from Secret Manager", e);
    } catch (Exception e) {
      LOGGER.error("Exception occurred while accessing Secret Manager: ", e);
      throw new RuntimeException("Unexpected error while reading credentials", e);
    }
  }
}
