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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import skyflow.core.ObjectMappers;

@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonDeserialize(builder = V1InsertRecordData.Builder.class)
public final class V1InsertRecordData {
  private final Optional<Map<String, Object>> data;

  private final Optional<Map<String, Object>> tokens;

  private final Map<String, Object> additionalProperties;

  private V1InsertRecordData(
      Optional<Map<String, Object>> data,
      Optional<Map<String, Object>> tokens,
      Map<String, Object> additionalProperties) {
    this.data = data;
    this.tokens = tokens;
    this.additionalProperties = additionalProperties;
  }

  /**
   * @return Columns names and values
   */
  @JsonProperty("data")
  public Optional<Map<String, Object>> getData() {
    return data;
  }

  /**
   * @return Tokens data for the columns if any
   */
  @JsonProperty("tokens")
  public Optional<Map<String, Object>> getTokens() {
    return tokens;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    return other instanceof V1InsertRecordData && equalTo((V1InsertRecordData) other);
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  private boolean equalTo(V1InsertRecordData other) {
    return data.equals(other.data) && tokens.equals(other.tokens);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.data, this.tokens);
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
    private Optional<Map<String, Object>> data = Optional.empty();

    private Optional<Map<String, Object>> tokens = Optional.empty();

    @JsonAnySetter private Map<String, Object> additionalProperties = new HashMap<>();

    private Builder() {}

    public Builder from(V1InsertRecordData other) {
      data(other.getData());
      tokens(other.getTokens());
      return this;
    }

    @JsonSetter(value = "data", nulls = Nulls.SKIP)
    public Builder data(Optional<Map<String, Object>> data) {
      this.data = data;
      return this;
    }

    public Builder data(Map<String, Object> data) {
      this.data = Optional.ofNullable(data);
      return this;
    }

    @JsonSetter(value = "tokens", nulls = Nulls.SKIP)
    public Builder tokens(Optional<Map<String, Object>> tokens) {
      this.tokens = tokens;
      return this;
    }

    public Builder tokens(Map<String, Object> tokens) {
      this.tokens = Optional.ofNullable(tokens);
      return this;
    }

    public V1InsertRecordData build() {
      return new V1InsertRecordData(data, tokens, additionalProperties);
    }
  }
}
