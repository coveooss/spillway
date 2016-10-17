/**
 * The MIT License
 * Copyright (c) 2016 Coveo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.coveo.spillway.limit;

import java.time.Instant;

import com.coveo.spillway.storage.AddAndGetRequest;

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
