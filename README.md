flume-rabbitmq-sink
===================

* Author: rinrinne a.k.a. rin_ne
* Repository: https://github.com/rinrinne/flume-rabbitmq-sink.git

Synopsis
-------------------

This is a flume sink for RabbitMQ. Mainly used as publisher for Gerrit Trigger.

* [Jenkins]
* [Gerrit Trigger]
* [RabbitMQ Consumer]

[Jenkins]: http://jenkins-ci.org/
[Gerrit Trigger]: https://wiki.jenkins-ci.org/display/JENKINS/Gerrit+Trigger
[RabbitMQ Consumer]: https://wiki.jenkins-ci.org/display/JENKINS/RabbitMQ+Consumer+Plugin

Environments
-------------------

* `linux`
  * `java-1.6`
    * `gradle`

Build
-------------------

TO build library with gradle.

    ./gradlew build

How to setup in agent config
-------------------

```ini
<Agent>.sinks = <Sink1>
<Agent>.sinks.<Sink1>.type = jp.glassmoon.flume.sink.rabbitmq.RabbitMQSink
```

Configuration
-------------------

**Bold** is string value.

|name              | default value
|:-----------------|:-----------------
|uri               | **amqp://localhost**
|username          | **guest**
|password          | **guest**
|exchange          | **gerrit.publish**
|routingKey        | **(Empty)**
|deliveryMode      | 1
|priority          | 0
|appId             | **(Empty)**
|contentEncoding   | **UTF-8**
|contentType       | **application/octet-stream**

If you use this together with [Gerrit Trigger], you must set `application/json` in `contentType`.

License
-------------------

The Apache Software License, Version 2.0

Copyright
-------------------

Copyright (c) 2014 rinrinne a.k.a. rin_ne
