# Spillway
[![Build Status](https://travis-ci.org/coveo/spillway.svg?branch=master)](https://travis-ci.org/coveo/spillway)
[![license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/coveo/spillway/blob/master/LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.coveo/spillway/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.coveo/spillway)

## A distributed throttling solution

Spillway is an easy to use solution to add distributed throttling at the software level in your public API.
This is particularly useful if multiple services are running in different JVMs.
It is also possible to quickly to react when throttling happens with our built-in call-back mechanism.

Storage backend currently supported:
- In memory (for usage within the same JVM)
- Redis

All external storage can be (and should be) wrapped in our asynchronous storage to avoid slowing down/stopping queries if external problems occurs with the external storage.

## Getting Started
#### Add Spillway to your project pom

```xml
<dependency>
    <groupId>com.coveo</groupId>
    <artifactId>spillway</artifactId>
    <version>2.0.0</version>
</dependency>
```

#### Documentation
The java documentation is available here: https://coveo.github.io/spillway/

#### Usage
###### Sample 1
```java
    LimitUsageStorage storage = new AsyncLimitUsageStorage(new RedisStorage("localhost"));
    SpillwayFactory spillwayFactory = new SpillwayFactory(storage);

    Limit<String> myLimit = LimitBuilder.of("myLimit").to(2).per(Duration.ofMinutes(1)).build();
    Spillway<String> spillway = spillwayFactory.enforce("myResource", myLimit);
    
    spillway.call("myLimit"); // nothing happens
    spillway.call("myLimit"); // nothing happens
    spillway.call("myLimit"); // throws SpillwayLimitExceededException
``` 

###### Sample 2
```java
    LimitUsageStorage storage = new InMemoryUsage();
    SpillwayFactory spillwayFactory = new SpillwayFactory(storage);

    Limit<User> userLimit = LimitBuilder.of("perUser", User::getName).to(3).per(Duration.ofHours(1)).build();
    Limit<User> ipLimit = LimitBuilder.of("perIp", User::getIp).to(3).per(Duration.ofHours(1)).withExceededCallback(myCallback).build();
    Spillway<User> spillway = spillwayFactory.enforce("myResource", userLimit, ipLimit);

    User john = new User("john", "127.0.0.1");
    User gina = new User("gina", "127.0.0.1");

    spillway.tryCall(john); // true
    spillway.tryCall(gina); // true
    spillway.tryCall(john); // true
    spillway.tryCall(gina); // false, perIp limit exceeded.
```

###### Sample 3
```java
    LimitUsageStorage storage = new InMemoryUsage();
    SpillwayFactory spillwayFactory = new SpillwayFactory(storage);
    
    LimitOverride override = LimitOverrideBuilder.of("john").to(10).per(Duration.ofHours(1)).build();
    Limit<String> userLimit = LimitBuilder.of("perUser").to(30).per(Duration.ofHours(1)).withLimitOverride(override).build();
    Spillway<User> spillway = spillwayFactory.enforce("myResource", userLimit);

    spillway.tryCall("john", 11); // false
    spillway.tryCall("gina", 20); // true
```
