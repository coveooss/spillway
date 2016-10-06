package com.coveo.spillway;

import java.time.Instant;

public class LimitKey {
  private String resource;
  private String limitName;
  private String property;
  private Instant bucket;

  public LimitKey(String resource, String limitName, String property, Instant bucket) {
    this.resource = resource;
    this.limitName = limitName;
    this.property = property;
    this.bucket = bucket;
  }

  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public String getProperty() {
    return property;
  }

  public void setProperty(String property) {
    this.property = property;
  }

  public Instant getBucket() {
    return bucket;
  }

  public void setBucket(Instant bucket) {
    this.bucket = bucket;
  }

  public String getLimitName() {
    return limitName;
  }

  public void setLimitName(String limitName) {
    this.limitName = limitName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LimitKey limitKey = (LimitKey) o;

    if (resource != null ? !resource.equals(limitKey.resource) : limitKey.resource != null)
      return false;
    if (limitName != null ? !limitName.equals(limitKey.limitName) : limitKey.limitName != null)
      return false;
    if (property != null ? !property.equals(limitKey.property) : limitKey.property != null)
      return false;
    return bucket != null ? bucket.equals(limitKey.bucket) : limitKey.bucket == null;
  }

  @Override
  public int hashCode() {
    int result = resource != null ? resource.hashCode() : 0;
    result = 31 * result + (limitName != null ? limitName.hashCode() : 0);
    result = 31 * result + (property != null ? property.hashCode() : 0);
    result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "LimitKey{"
        + "resource='"
        + resource
        + '\''
        + ", limitName='"
        + limitName
        + '\''
        + ", property='"
        + property
        + '\''
        + ", bucket="
        + bucket
        + '}';
  }

  public static LimitKey fromRequest(AddAndGetRequest request) {
    return new LimitKey(
        request.getResource(), request.getLimitName(), request.getProperty(), request.getBucket());
  }
}
