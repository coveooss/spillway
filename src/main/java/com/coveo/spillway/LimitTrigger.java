package com.coveo.spillway;

public interface LimitTrigger {
  <T> void callbackIfRequired(
      T context, int cost, int currentValue, LimitDefinition limitDefinition);
}
