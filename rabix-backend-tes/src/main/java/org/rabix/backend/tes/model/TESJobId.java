package org.rabix.backend.tes.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TESJobId {

  @JsonProperty("id")
  private String value;

  @JsonCreator
  public TESJobId(@JsonProperty("id") String value) {
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
    return "TESJobId [value=" + value + "]";
  }
  
}
