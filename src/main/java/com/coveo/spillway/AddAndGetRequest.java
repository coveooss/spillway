package com.coveo.spillway;

import java.time.Duration;
import java.time.Instant;

public class AddAndGetRequest {
  private String resource;
  private String limitName;
  private String property;
  private Duration expiration;
  private Instant eventTimestamp;
  private int incrementBy;

  public String getResource() {
    return resource;
  }

  public String getLimitName() {
    return limitName;
  }

  public String getProperty() {
    return property;
  }

  public Duration getExpiration() {
    return expiration;
  }

  public Instant getEventTimestamp() {
    return eventTimestamp;
  }

  public int getIncrementBy() {
    return incrementBy;
  }

  private AddAndGetRequest(Builder builder) {
    resource = builder.resource;
    limitName = builder.limitName;
    property = builder.property;
    expiration = builder.expiration;
    eventTimestamp = builder.eventTimestamp;
    incrementBy = builder.incrementBy;
  }

  public AddAndGetRequest withResource(String resource) {
    this.resource = resource;
    return this;
  }

  public static final class Builder {
    private String resource;
    private String limitName;
    private String property;
    private Duration expiration;
    private Instant eventTimestamp;
    private int incrementBy = 1;

    public Builder() {}

    public Builder withResource(String val) {
      resource = val;
      return this;
    }

    public Builder withLimitName(String val) {
      limitName = val;
      return this;
    }

    public Builder withProperty(String val) {
      property = val;
      return this;
    }

    public Builder withExpiration(Duration val) {
      expiration = val;
      return this;
    }

    public Builder withEventTimestamp(Instant val) {
      eventTimestamp = val;
      return this;
    }

    public Builder withIncrementBy(int val) {
      incrementBy = val;
      return this;
    }

    public AddAndGetRequest build() {
      return new AddAndGetRequest(this);
    }
  }
}
