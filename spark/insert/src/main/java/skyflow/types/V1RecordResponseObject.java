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
@JsonDeserialize(builder = V1RecordResponseObject.Builder.class)
public final class V1RecordResponseObject {
  private final Optional<String> skyflowId;

  private final Optional<Map<String, Object>> tokens;

  private final Optional<Map<String, Object>> data;

  private final Optional<Map<String, Object>> hashedData;

  private final Optional<String> error;

  private final Optional<Integer> httpCode;

  private final Map<String, Object> additionalProperties;

  private V1RecordResponseObject(
      Optional<String> skyflowId,
      Optional<Map<String, Object>> tokens,
      Optional<Map<String, Object>> data,
      Optional<Map<String, Object>> hashedData,
      Optional<String> error,
      Optional<Integer> httpCode,
      Map<String, Object> additionalProperties) {
    this.skyflowId = skyflowId;
    this.tokens = tokens;
    this.data = data;
    this.hashedData = hashedData;
    this.error = error;
    this.httpCode = httpCode;
    this.additionalProperties = additionalProperties;
  }

  /**
   * @return Skyflow ID for the inserted record
   */
  @JsonProperty("skyflowID")
  public Optional<String> getSkyflowId() {
    return skyflowId;
  }

  /**
   * @return Tokens data for the columns if any
   */
  @JsonProperty("tokens")
  public Optional<Map<String, Object>> getTokens() {
    return tokens;
  }

  /**
   * @return Columns names and values
   */
  @JsonProperty("data")
  public Optional<Map<String, Object>> getData() {
    return data;
  }

  /**
   * @return Hashed Data for the columns if any
   */
  @JsonProperty("hashedData")
  public Optional<Map<String, Object>> getHashedData() {
    return hashedData;
  }

  /**
   * @return Partial Error message if any
   */
  @JsonProperty("error")
  public Optional<String> getError() {
    return error;
  }

  /**
   * @return HTTP status code of the response
   */
  @JsonProperty("httpCode")
  public Optional<Integer> getHttpCode() {
    return httpCode;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    return other instanceof V1RecordResponseObject && equalTo((V1RecordResponseObject) other);
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  private boolean equalTo(V1RecordResponseObject other) {
    return skyflowId.equals(other.skyflowId)
        && tokens.equals(other.tokens)
        && data.equals(other.data)
        && hashedData.equals(other.hashedData)
        && error.equals(other.error)
        && httpCode.equals(other.httpCode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.skyflowId, this.tokens, this.data, this.hashedData, this.error, this.httpCode);
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
    private Optional<String> skyflowId = Optional.empty();

    private Optional<Map<String, Object>> tokens = Optional.empty();

    private Optional<Map<String, Object>> data = Optional.empty();

    private Optional<Map<String, Object>> hashedData = Optional.empty();

    private Optional<String> error = Optional.empty();

    private Optional<Integer> httpCode = Optional.empty();

    @JsonAnySetter private Map<String, Object> additionalProperties = new HashMap<>();

    private Builder() {}

    public Builder from(V1RecordResponseObject other) {
      skyflowId(other.getSkyflowId());
      tokens(other.getTokens());
      data(other.getData());
      hashedData(other.getHashedData());
      error(other.getError());
      httpCode(other.getHttpCode());
      return this;
    }

    @JsonSetter(value = "skyflowID", nulls = Nulls.SKIP)
    public Builder skyflowId(Optional<String> skyflowId) {
      this.skyflowId = skyflowId;
      return this;
    }

    public Builder skyflowId(String skyflowId) {
      this.skyflowId = Optional.ofNullable(skyflowId);
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

    @JsonSetter(value = "data", nulls = Nulls.SKIP)
    public Builder data(Optional<Map<String, Object>> data) {
      this.data = data;
      return this;
    }

    public Builder data(Map<String, Object> data) {
      this.data = Optional.ofNullable(data);
      return this;
    }

    @JsonSetter(value = "hashedData", nulls = Nulls.SKIP)
    public Builder hashedData(Optional<Map<String, Object>> hashedData) {
      this.hashedData = hashedData;
      return this;
    }

    public Builder hashedData(Map<String, Object> hashedData) {
      this.hashedData = Optional.ofNullable(hashedData);
      return this;
    }

    @JsonSetter(value = "error", nulls = Nulls.SKIP)
    public Builder error(Optional<String> error) {
      this.error = error;
      return this;
    }

    public Builder error(String error) {
      this.error = Optional.ofNullable(error);
      return this;
    }

    @JsonSetter(value = "httpCode", nulls = Nulls.SKIP)
    public Builder httpCode(Optional<Integer> httpCode) {
      this.httpCode = httpCode;
      return this;
    }

    public Builder httpCode(Integer httpCode) {
      this.httpCode = Optional.ofNullable(httpCode);
      return this;
    }

    public V1RecordResponseObject build() {
      return new V1RecordResponseObject(
          skyflowId, tokens, data, hashedData, error, httpCode, additionalProperties);
    }
  }
}
