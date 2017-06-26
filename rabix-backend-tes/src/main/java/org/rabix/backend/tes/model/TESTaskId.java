package org.rabix.backend.tes.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TESTaskId {

  @JsonProperty("value")
  private String value;

  @JsonCreator
  public TESTaskId(@JsonProperty("value") String value) {
    this.value = value;
  }
  
  public String getValue() {
    return value;
  }
  
  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "TESTaskID [value=" + value + "]";
  }
  
}
