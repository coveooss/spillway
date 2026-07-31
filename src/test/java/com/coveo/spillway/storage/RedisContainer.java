/*
 * Copyright (c) Coveo Solutions Inc.
 */
package com.coveo.spillway.storage;

import java.util.Collections;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A barebones Redis container, using the <a href="https://hub.docker.com/_/redis">Docker official (community) image</a>.
 */
public class RedisContainer<SELF extends GenericContainer<SELF>> extends GenericContainer<SELF> {
  private static final DockerImageName REDIS_IMAGE =
      DockerImageName.parse("redis").withTag("8.10.0-alpine");
  private static final int REDIS_PORT = 6379;

  public RedisContainer() {
    super(REDIS_IMAGE);
    setExposedPorts(Collections.singletonList(REDIS_PORT));
  }

  public int getMappedRedisPort() {
    return getMappedPort(REDIS_PORT);
  }
}
