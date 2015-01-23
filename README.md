flume-ng-kafka-sink
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with [kafka 0.8.0](http://kafka.apache.org/08/quickstart.html).

Configuration of Kafka Sink
----------

    agent.sinks.kafkaSink.type = org.apache.flume.sink.kafka.KafkaSink
    agent.sinks.kafkaSink.partitioner.class = kafka.producer.DefaultPartitioner
    agent.sinks.kafkaSink.metadata.broker.list = hadoop-manager:9092,hadoop-node1:9092,hadoop-node2:9092
    agent.sinks.kafkaSink.topic = flume
    agent.sinks.kafkaSink.batch.num.messages = 200
    
    # In async producer:
    agent.sinks.kafkaSink.producer.type = async
    agent.sinks.kafkaSink.queue.buffering.max.ms = 5000
    agent.sinks.kafkaSink.queue.buffering.max.messages = 10000
    agent.sinks.kafkaSink.queue.enqueue.timeout.ms = -1
    agent.sinks.kafkaSink.serializer.class = kafka.serializer.DefaultEncoder
    agent.sinks.kafkaSink.key.serializer.class = kafka.serializer.DefaultEncoder

Special Thanks
---------

In fact I'm a newbie in Java. I have learnt a lot from [flumg-ng-rabbitmq](https://github.com/jcustenborder/flume-ng-rabbitmq). Thanks to [Jeremy Custenborder](https://github.com/jcustenborder).

