// Copyright (c) 2025 Skyflow, Inc.

package skyflow.resources.service.requests;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import skyflow.core.ObjectMappers;
import skyflow.types.V1InsertRecordData;
import skyflow.types.V1Upsert;

@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonDeserialize(builder = V1InsertRequest.Builder.class)
public final class V1InsertRequest {
  private final Optional<String> vaultId;

  private final Optional<String> tableName;

  private final Optional<List<V1InsertRecordData>> records;

  private final Optional<V1Upsert> upsert;

  private final Map<String, Object> additionalProperties;

  private V1InsertRequest(
      Optional<String> vaultId,
      Optional<String> tableName,
      Optional<List<V1InsertRecordData>> records,
      Optional<V1Upsert> upsert,
      Map<String, Object> additionalProperties) {
    this.vaultId = vaultId;
    this.tableName = tableName;
    this.records = records;
    this.upsert = upsert;
    this.additionalProperties = additionalProperties;
  }

  /**
   * @return ID of the vault where data is being inserted
   */
  @JsonProperty("vaultID")
  public Optional<String> getVaultId() {
    return vaultId;
  }

  /**
   * @return Name of the table where data is being inserted
   */
  @JsonProperty("tableName")
  public Optional<String> getTableName() {
    return tableName;
  }

  /**
   * @return List of data row wise that is to be inserted in the vault
   */
  @JsonProperty("records")
  public Optional<List<V1InsertRecordData>> getRecords() {
    return records;
  }

  @JsonProperty("upsert")
  public Optional<V1Upsert> getUpsert() {
    return upsert;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    return other instanceof V1InsertRequest && equalTo((V1InsertRequest) other);
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  private boolean equalTo(V1InsertRequest other) {
    return vaultId.equals(other.vaultId)
        && tableName.equals(other.tableName)
        && records.equals(other.records)
        && upsert.equals(other.upsert);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.vaultId, this.tableName, this.records, this.upsert);
  }

  @Override
  public String toString() {
    return ObjectMappers.stringify(this);
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Builder {
    private Optional<String> vaultId = Optional.empty();

    private Optional<String> tableName = Optional.empty();

    private Optional<List<V1InsertRecordData>> records = Optional.empty();

    private Optional<V1Upsert> upsert = Optional.empty();

    @JsonAnySetter private Map<String, Object> additionalProperties = new HashMap<>();

    private Builder() {}

    public Builder from(V1InsertRequest other) {
      vaultId(other.getVaultId());
      tableName(other.getTableName());
      records(other.getRecords());
      upsert(other.getUpsert());
      return this;
    }

    @JsonSetter(value = "vaultID", nulls = Nulls.SKIP)
    public Builder vaultId(Optional<String> vaultId) {
      this.vaultId = vaultId;
      return this;
    }

    public Builder vaultId(String vaultId) {
      this.vaultId = Optional.ofNullable(vaultId);
      return this;
    }

    @JsonSetter(value = "tableName", nulls = Nulls.SKIP)
    public Builder tableName(Optional<String> tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = Optional.ofNullable(tableName);
      return this;
    }

    @JsonSetter(value = "records", nulls = Nulls.SKIP)
    public Builder records(Optional<List<V1InsertRecordData>> records) {
      this.records = records;
      return this;
    }

    public Builder records(List<V1InsertRecordData> records) {
      this.records = Optional.ofNullable(records);
      return this;
    }

    @JsonSetter(value = "upsert", nulls = Nulls.SKIP)
    public Builder upsert(Optional<V1Upsert> upsert) {
      this.upsert = upsert;
      return this;
    }

    public Builder upsert(V1Upsert upsert) {
      this.upsert = Optional.ofNullable(upsert);
      return this;
    }

    public V1InsertRequest build() {
      return new V1InsertRequest(vaultId, tableName, records, upsert, additionalProperties);
    }
  }
}
