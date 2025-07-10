// Copyright (c) 2025 Skyflow, Inc.

package skyflow.types;

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

@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonDeserialize(builder = V1Upsert.Builder.class)
public final class V1Upsert {
  private final Optional<EnumUpdateType> updateType;

  private final Optional<List<String>> uniqueColumns;

  private final Map<String, Object> additionalProperties;

  private V1Upsert(
      Optional<EnumUpdateType> updateType,
      Optional<List<String>> uniqueColumns,
      Map<String, Object> additionalProperties) {
    this.updateType = updateType;
    this.uniqueColumns = uniqueColumns;
    this.additionalProperties = additionalProperties;
  }

  @JsonProperty("updateType")
  public Optional<EnumUpdateType> getUpdateType() {
    return updateType;
  }

  /**
   * @return Name of a unique columns in the table. Uses upsert operations to check if a record
   *     exists based on the unique column's value. If a matching record exists, the record updates
   *     with the values you provide. If a matching record doesn't exist, the upsert operation
   *     inserts a new record.
   */
  @JsonProperty("uniqueColumns")
  public Optional<List<String>> getUniqueColumns() {
    return uniqueColumns;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    return other instanceof V1Upsert && equalTo((V1Upsert) other);
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  private boolean equalTo(V1Upsert other) {
    return updateType.equals(other.updateType) && uniqueColumns.equals(other.uniqueColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.updateType, this.uniqueColumns);
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
    private Optional<EnumUpdateType> updateType = Optional.empty();

    private Optional<List<String>> uniqueColumns = Optional.empty();

    @JsonAnySetter private Map<String, Object> additionalProperties = new HashMap<>();

    private Builder() {}

    public Builder from(V1Upsert other) {
      updateType(other.getUpdateType());
      uniqueColumns(other.getUniqueColumns());
      return this;
    }

    @JsonSetter(value = "updateType", nulls = Nulls.SKIP)
    public Builder updateType(Optional<EnumUpdateType> updateType) {
      this.updateType = updateType;
      return this;
    }

    public Builder updateType(EnumUpdateType updateType) {
      this.updateType = Optional.ofNullable(updateType);
      return this;
    }

    @JsonSetter(value = "uniqueColumns", nulls = Nulls.SKIP)
    public Builder uniqueColumns(Optional<List<String>> uniqueColumns) {
      this.uniqueColumns = uniqueColumns;
      return this;
    }

    public Builder uniqueColumns(List<String> uniqueColumns) {
      this.uniqueColumns = Optional.ofNullable(uniqueColumns);
      return this;
    }

    public V1Upsert build() {
      return new V1Upsert(updateType, uniqueColumns, additionalProperties);
    }
  }
}
