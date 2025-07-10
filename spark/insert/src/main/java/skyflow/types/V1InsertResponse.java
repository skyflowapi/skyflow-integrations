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
@JsonDeserialize(builder = V1InsertResponse.Builder.class)
public final class V1InsertResponse {
  private final Optional<List<V1RecordResponseObject>> records;

  private final Map<String, Object> additionalProperties;

  private V1InsertResponse(
      Optional<List<V1RecordResponseObject>> records, Map<String, Object> additionalProperties) {
    this.records = records;
    this.additionalProperties = additionalProperties;
  }

  /**
   * @return List of inserted records with skyflow ID, tokens, data, and any partial errors.
   */
  @JsonProperty("records")
  public Optional<List<V1RecordResponseObject>> getRecords() {
    return records;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    return other instanceof V1InsertResponse && equalTo((V1InsertResponse) other);
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  private boolean equalTo(V1InsertResponse other) {
    return records.equals(other.records);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.records);
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
    private Optional<List<V1RecordResponseObject>> records = Optional.empty();

    @JsonAnySetter private Map<String, Object> additionalProperties = new HashMap<>();

    private Builder() {}

    public Builder from(V1InsertResponse other) {
      records(other.getRecords());
      return this;
    }

    @JsonSetter(value = "records", nulls = Nulls.SKIP)
    public Builder records(Optional<List<V1RecordResponseObject>> records) {
      this.records = records;
      return this;
    }

    public Builder records(List<V1RecordResponseObject> records) {
      this.records = Optional.ofNullable(records);
      return this;
    }

    public V1InsertResponse build() {
      return new V1InsertResponse(records, additionalProperties);
    }
  }
}
