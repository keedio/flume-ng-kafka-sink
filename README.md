# flume-ng-demo-kafka-sink

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with [kafka 0.8.1.1](http://kafka.apache.org/08/quickstart.html).

Aditionally to original plugin, this one implements a dynamic kafka topic selection method. It's used an ExtraData field in flume event body to build the kafka destintation topic.

This plugin depends on [flume-enrichment-interceptor](https://github.com/keedio/flume-enrichment-interceptor-skeleton)

## Build

```mvn package```

## Deploy 

You have to copy the .jar library generated in target to the plugins.d lib folder of your flume installation  
(e.g /usr/lib/flume/plugins.d/flume-ng-demo-kafka-sink/lib
and the .jar libraries in target/libs in plugins.d libext folder  
(e.g /plugins.d/flume-ng-demo-kafka-sink/libext)

## Working mode: 

To build the destination topic dynamicTopic configuration parameter is used.  
The format of this parameter is key1-key2..., so it's search in the ExtraData field both keys and builds the topic with them
    
### Example:

- "dynamicTopic = hostname-domain"  
- Flume eventBody = { "extraData":{"hostname": "localhost", "domain": "localdomain"}, "message": "the original body string"}  
  
The destination topic to be build will be <b>"localhost-localdomain"</b>  
If this topic doesn't exists the defaultTopic will be used as destination topic.
    

### Configuration example of Kafka Demo Sink

```ini
    # In async producer:
    agent.sinks.kafka-sink.channel = memory-channel
    agent.sinks.kafka-sink.type = org.keedio.flume.sink.KafkaSink
    agent.sinks.kafka-sink.zk.connect = hadoop-manager:2181,hadoop-node1:2181,hadoop-node2:2181
    agent.sinks.kafka-sink.defaultTopic = default
    agent.sinks.kafka-sink.dynamicTopic = hostname-item
    agent.sinks.kafka-sink.batch.num.messages = 1000
    agent.sinks.kafka-sink.queue.buffering.max.ms = 1000
    agent.sinks.kafka-sink.producer.type = async
    agent.sinks.kafka-sink.metadata.broker.list = hadoop-manager:9092,hadoop-node1:9092,hadoop-node2:9092
```
