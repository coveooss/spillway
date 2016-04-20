package com.coveo.spillway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SpillwayLimitExceededException extends SpillwayException {

  private List<LimitDefinition> exceededLimits = new ArrayList<>();
  private Object context;

  public SpillwayLimitExceededException(LimitDefinition limitDefinition, Object context) {
    super("Limits " + limitDefinition + " exceeded.");
    exceededLimits.add(limitDefinition);
    this.context = context;
  }

  public SpillwayLimitExceededException(List<LimitDefinition> limitDefinitions, Object context) {
    super("Limits " + limitDefinitions + " exceeded.");
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
