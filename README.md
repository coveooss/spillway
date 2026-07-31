# Spillway

[![license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/coveo/spillway/blob/master/LICENSE)

## A distributed throttling solution

Spillway is an easy to use solution to add distributed throttling at the software level in your public API. This is
particularly useful if multiple services are running in different JVMs. It is also possible to quickly to react when
throttling happens with our built-in call-back mechanism.

Storage backend currently supported:

- In memory (for usage within the same JVM)
- Redis

All external storage can be (and should be) wrapped in our asynchronous storage to avoid slowing down/stopping queries
if external problems occurs with the external storage.

## Getting Started

#### Add Spillway to your project pom

```xml

<dependency>
    <groupId>com.coveo</groupId>
    <artifactId>spillway</artifactId>
    <version>4.0.0</version>
</dependency>
```

#### Usage

###### Sample 1

```java
import java.time.Duration;

import com.coveo.spillway.Spillway;
import com.coveo.spillway.SpillwayFactory;
import com.coveo.spillway.limit.Limit;
import com.coveo.spillway.limit.LimitBuilder;
import com.coveo.spillway.storage.AsyncLimitUsageStorage;
import com.coveo.spillway.storage.LimitUsageStorage;
import com.coveo.spillway.storage.RedisStorage;

import redis.clients.jedis.RedisClient;

class Sample
{
    static void main(String[] argv)
    {
        LimitUsageStorage storage = new AsyncLimitUsageStorage(new RedisStorage(RedisClient.create("127.0.0.1", 6379)));
        SpillwayFactory spillwayFactory = new SpillwayFactory(storage);

        Limit<String> myLimit = LimitBuilder.of("myLimit").to(2).per(Duration.ofMinutes(1)).build();
        Spillway<String> spillway = spillwayFactory.enforce("myResource", myLimit);

        spillway.call("myLimit"); // nothing happens
        spillway.call("myLimit"); // nothing happens
        spillway.call("myLimit"); // throws SpillwayLimitExceededException
    }
}
``` 

###### Sample 2

```java
import java.time.Duration;

import com.coveo.spillway.Spillway;
import com.coveo.spillway.SpillwayFactory;
import com.coveo.spillway.limit.Limit;
import com.coveo.spillway.storage.InMemoryStorage;
import com.coveo.spillway.storage.LimitUsageStorage;

class Sample
{
    static void main(String[] argv)
    {
        LimitUsageStorage storage = new InMemoryStorage();
        SpillwayFactory spillwayFactory = new SpillwayFactory(storage);

        Limit<User> userLimit = LimitBuilder.of("perUser", User::getName).to(3).per(Duration.ofHours(1)).build();
        Limit<User> ipLimit = LimitBuilder.of("perIp", User::getIp).to(3).per(Duration.ofHours(1)).withExceededCallback(
                myCallback).build();
        Spillway<User> spillway = spillwayFactory.enforce("myResource", userLimit, ipLimit);

        User john = new User("john", "127.0.0.1");
        User gina = new User("gina", "127.0.0.1");

        spillway.tryCall(john); // true
        spillway.tryCall(gina); // true
        spillway.tryCall(john); // true
        spillway.tryCall(gina); // false, perIp limit exceeded.
    }
}
```

###### Sample 3

```java
import com.coveo.spillway.Spillway;
import com.coveo.spillway.SpillwayFactory;
import com.coveo.spillway.limit.Limit;
import com.coveo.spillway.limit.LimitBuilder;
import com.coveo.spillway.limit.override.LimitOverride;
import com.coveo.spillway.limit.override.LimitOverrideBuilder;
import com.coveo.spillway.storage.LimitUsageStorage;

class Sample
{
    static void main(String[] argv)
    {
        LimitUsageStorage storage = new InMemoryStorage();
        SpillwayFactory spillwayFactory = new SpillwayFactory(storage);

        LimitOverride override = LimitOverrideBuilder.of("john").to(10).per(Duration.ofHours(1)).build();
        Limit<String> userLimit = LimitBuilder.of("perUser").to(30).per(Duration.ofHours(1)).withLimitOverride(override)
                                              .build();
        Spillway<User> spillway = spillwayFactory.enforce("myResource", userLimit);

        spillway.tryCall("john", 11); // false
        spillway.tryCall("gina", 20); // true
    }
}
```

## External Resources

[cirrus-up-cloud](https://github.com/cirrus-up-cloud) wrote
a [nice blog post](https://www.cirrusup.cloud/limit-accepted-requests-using-aws-elasticache/) about using Spillway on
AWS with Elasticache.
