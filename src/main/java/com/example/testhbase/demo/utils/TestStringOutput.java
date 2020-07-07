package com.example.testhbase.demo.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;

public class TestStringOutput implements Serializable, Writable {

  private final Map<String, String> output;

  public TestStringOutput(Map<String, String> op) {
    output = op;
  }


  @Override
  public String toString() {
    try {
      return new ObjectMapper().writeValueAsString(output);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }


  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.toString().getBytes().length);
    out.write(this.toString().getBytes(), 0, this.toString().getBytes().length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    in.readFully(this.toString().getBytes(), 0, this.toString().getBytes().length);
  }
}
