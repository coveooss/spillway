package com.coveo.spillway.memory;

import com.coveo.spillway.LimitKey;

import java.time.Instant;

public class OverrideKeyRequest {
  private LimitKey limitKey;

  public OverrideKeyRequest(LimitKey limitKey, Instant expirationDate, int newValue) {
    this.limitKey = limitKey;
    this.expirationDate = expirationDate;
    this.newValue = newValue;
  }

  private Instant expirationDate;
  private int newValue;

  public LimitKey getLimitKey() {
    return limitKey;
  }

  public Instant getExpirationDate() {
    return expirationDate;
  }

  public int getNewValue() {
    return newValue;
  }
}
