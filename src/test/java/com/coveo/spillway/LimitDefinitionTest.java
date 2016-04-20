package com.coveo.spillway;

import org.junit.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;


public class LimitDefinitionTest {

  @Test
  public void toStringLooksGood() {
    String toString = new LimitDefinition("perUser", 5, Duration.ofHours(5)).toString();

    assertThat(toString).isEqualTo("perUser[5 calls/PT5H]");
  }
}
