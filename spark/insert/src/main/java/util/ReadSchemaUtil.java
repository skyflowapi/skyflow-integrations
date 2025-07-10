// Copyright (c) 2025 Skyflow, Inc.

package util;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadSchemaUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadSchemaUtil.class);

  /**
   * Reads a Spark schema from a Google Cloud Storage location. The schema should be stored as a
   * JSON file in the GCS bucket.
   *
   * @param schemaUrl The GCS URL of the schema file (format: gs://bucket-name/path/to/schema.json)
   * @return StructType representing the Spark schema loaded from the JSON file
   * @throws IllegalArgumentException if the schema URL is invalid or the file cannot be read
   */
  public static StructType readSchema(String schemaUrl) {

    StructType schema = null;

    String[] split_url = schemaUrl.replace("gs://", "").split("/", 2);
    String bucket = split_url[0];
    String objectUrl = split_url[1];

    Storage storage = StorageOptions.getDefaultInstance().getService();

    Blob blob = storage.get(bucket, objectUrl);
    String schemaSource = new String(blob.getContent());
    schema = (StructType) DataType.fromJson(schemaSource);

    return schema;
  }
}
