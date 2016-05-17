package com.coveo.spillway;

import java.time.Duration;
import java.time.Instant;

public class AddAndGetRequest {
  private String resource;
  private String limitName;
  private String property;
  private Duration expiration;
  private Instant eventTimestamp;
  private int cost;

  private Instant bucket;

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

  public int getCost() {
    return cost;
  }

  public Instant getBucket() {
    return bucket;
  }

  private AddAndGetRequest(Builder builder) {
    resource = builder.resource;
    limitName = builder.limitName;
    property = builder.property;
    expiration = builder.expiration;
    eventTimestamp = builder.eventTimestamp;
    cost = builder.cost;
    bucket =
        Instant.ofEpochMilli(
            (eventTimestamp.toEpochMilli() / expiration.toMillis()) * expiration.toMillis());
  }

  public static final class Builder {
    private String resource;
    private String limitName;
    private String property;
    private Duration expiration;
    private Instant eventTimestamp;
    private int cost = 1;

    public Builder() {}

    public Builder(AddAndGetRequest other) {
      this.resource = other.resource;
      this.limitName = other.limitName;
      this.property = other.property;
      this.expiration = other.expiration;
      this.eventTimestamp = other.eventTimestamp;
      this.cost = other.cost;
    }

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

    public Builder withCost(int val) {
      cost = val;
      return this;
    }

    public AddAndGetRequest build() {
      return new AddAndGetRequest(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AddAndGetRequest that = (AddAndGetRequest) o;

    if (cost != that.cost) return false;
    if (resource != null ? !resource.equals(that.resource) : that.resource != null) return false;
    if (limitName != null ? !limitName.equals(that.limitName) : that.limitName != null)
      return false;
    if (property != null ? !property.equals(that.property) : that.property != null) return false;
    if (expiration != null ? !expiration.equals(that.expiration) : that.expiration != null)
      return false;
    if (eventTimestamp != null
        ? !eventTimestamp.equals(that.eventTimestamp)
        : that.eventTimestamp != null) return false;
    return bucket != null ? bucket.equals(that.bucket) : that.bucket == null;
  }

  @Override
  public int hashCode() {
    int result = resource != null ? resource.hashCode() : 0;
    result = 31 * result + (limitName != null ? limitName.hashCode() : 0);
    result = 31 * result + (property != null ? property.hashCode() : 0);
    result = 31 * result + (expiration != null ? expiration.hashCode() : 0);
    result = 31 * result + (eventTimestamp != null ? eventTimestamp.hashCode() : 0);
    result = 31 * result + cost;
    result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "AddAndGetRequest{"
        + "resource='"
        + resource
        + '\''
        + ", limitName='"
        + limitName
        + '\''
        + ", property='"
        + property
        + '\''
        + ", expiration="
        + expiration
        + ", eventTimestamp="
        + eventTimestamp
        + ", cost="
        + cost
        + ", bucket="
        + bucket
        + '}';
  }
}
