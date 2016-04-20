package com.coveo.spillway;

import org.junit.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;

public class LimitTest {

  @Test
  public void toStringIsTheLimitDefinitionToString() {
    LimitDefinition limitDefinition = new LimitDefinition("potato", 5, Duration.ofDays(100));
    Limit<String> limit = new Limit<>(limitDefinition, Function.identity(), Optional.empty());

    assertThat(limit.toString()).isEqualTo(limitDefinition.toString());
  }
}
