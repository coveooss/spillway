package com.coveo.spillway;

public class LimitTrigger {
  private int limitValue;
  private LimitTriggerCallback callback;

  public LimitTriggerCallback getCallback() {
    return callback;
  }

  public int getLimitValue() {
    return limitValue;
  }

  public LimitTrigger(int limitValue, LimitTriggerCallback callback) {
    this.limitValue = limitValue;
    this.callback = callback;
  }
}
