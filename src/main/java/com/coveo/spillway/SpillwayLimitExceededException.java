package com.coveo.spillway;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SpillwayLimitExceededException extends SpillwayException {

  private List<LimitDefinition> exceededLimits = new ArrayList<>();
  private Object context;

  public SpillwayLimitExceededException(LimitDefinition limitDefinition, Object context, int cost) {
    this(Arrays.asList(limitDefinition), context, cost);
  }

  public SpillwayLimitExceededException(
      List<LimitDefinition> limitDefinitions, Object context, int cost) {
    super(
        "Attempted to use " + cost + " units in limit " + limitDefinitions + " but it exceeds it.");
    exceededLimits.addAll(limitDefinitions);
    this.context = context;
  }

  public List<LimitDefinition> getExceededLimits() {
    return Collections.unmodifiableList(exceededLimits);
  }

  public Object getContext() {
    return context;
  }
}
