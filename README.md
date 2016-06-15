[![Build Status](https://travis-ci.org/coveo/spillway.svg?branch=master)](https://travis-ci.org/coveo/spillway)
[![license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/coveo/spillway/blob/master/LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.coveo/spillway/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.coveo/spillway)

Sample usage

```java
    LimitUsageStorage storage = new RedisStorage("localhost");
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
