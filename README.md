flume-ng-kafka-sink
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with [kafka 0.8.0](http://kafka.apache.org/08/quickstart.html).

Configuration of Kafka Sink
----------

    agent_log.sinks.kafka.type = org.apache.flume.sink.kafka.KafkaSink
    agent_log.sinks.kafka.channel = all_channel
    agent_log.sinks.kafka.batch.num.messages = 200
    agent_log.sinks.kafka.queue.buffering.max.ms = 5000
    agent_log.sinks.kafka.producer.type = async
    agent_log.sinks.kafka.serializer.class = kafka.serializer.StringEncoder

Speical Thanks
---------

In fact I'm a newbie in Java. I have learnt a lot from [flumg-ng-rabbitmq](https://github.com/jcustenborder/flume-ng-rabbitmq). Thanks to [Jeremy Custenborder](https://github.com/jcustenborder).

