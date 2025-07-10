// Copyright (c) 2025 Skyflow, Inc.

package skyflow.types;

import com.fasterxml.jackson.annotation.JsonValue;

public enum EnumUpdateType {
  UPDATE("UPDATE"),

  REPLACE("REPLACE");

  private final String value;

  EnumUpdateType(String value) {
    this.value = value;
  }

  @JsonValue
  @Override
  public String toString() {
    return this.value;
  }
}
