# Spillway
[![Build Status](https://travis-ci.org/coveo/spillway.svg?branch=master)](https://travis-ci.org/coveo/spillway)
[![license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/coveo/spillway/blob/master/LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.coveo/spillway/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.coveo/spillway)

## A distributed throttling solution

Spillway is an easy to use solution to add distributed throttling at the software level in your public API.
This is particularly useful if multiple services are running in different JVM.
It is also possible to quickly to react when throlling happens with our built-in call-back mechanism.

Storage currently supported:
- In memory (for usage in the same JVM)
- Redis

All external storage can be (and should be) wrapped in our asynchronous storage to avoid slowing down/stopping queries if external problems occurs with the external storage.

## Getting Started
#### Add Spillway to your project pom

```xml
<dependency>
    <groupId>com.coveo</groupId>
    <artifactId>spillway</artifactId>
    <version>1.0.0</version>
</dependency>
```

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
