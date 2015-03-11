/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.apache.flume.sink.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.kafka.KafkaDynamicSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sink of Kafka which get events from channels and publish to Kafka. I use
 * this in our company production environment which can hit 100k messages per
 * second. <tt>zk.connect: </tt> the zookeeper ip kafka use.
 * <p>
 * <tt>topic: </tt> the topic to read from kafka.
 * <p>
 * <tt>batchSize: </tt> send serveral messages in one request to kafka.
 * <p>
 * <tt>producer.type: </tt> type of producer of kafka, async or sync is
 * available.<o> <tt>serializer.class: </tt>{@kafka.serializer.StringEncoder
 * 
 * 
 * 
 * }
 */
public class KafkaDynamicTopicSink extends AbstractSink implements Configurable {
	private static final Logger log = LoggerFactory
			.getLogger(KafkaDynamicTopicSink.class);
	private String defaultTopic, dynamicTopic;
	private Producer<byte[], byte[]> producer;
	private KafkaDynamicSinkCounter counter;

	public Status process() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		String destTopic = "";
		try {
			tx.begin();
			Event event = channel.take();
			if (event == null) {
				tx.commit();
				return Status.READY;
			}
			try {
				destTopic = KafkaDynamicSinkUtil.getDestinationTopic(dynamicTopic, defaultTopic, event.getBody());
				
				log.debug("Destination topic: {}", destTopic);
				producer.send(new KeyedMessage<byte[], byte[]>(destTopic, event.getBody()));
				counter.increaseCounterMessageSent();
            } catch (Exception e) {
                counter.increaseCounterMessageSentError();
                throw e;
            }

            log.debug("Message: {}", new String(event.getBody()));
            tx.commit();
            return Status.READY;

        } catch (Exception e) {
            log.error("KafkaSink Exception:{}", e);

            try {
				tx.rollback();
			} catch (Exception e2) {
				log.error("Rollback Exception:{}", e2);
			}
			return Status.BACKOFF;
		} finally {
			tx.close();
		}
	}

	public void configure(Context context) {
		defaultTopic = context.getString("defaultTopic");
		if (defaultTopic == null) {
			throw new ConfigurationException(
					"Kafka default topic must be specified.");
		}
		dynamicTopic = context.getString("dynamicTopic");

		producer = KafkaDynamicSinkUtil.getProducer(context);
		counter = new KafkaDynamicSinkCounter("SINK.Kafka-" + getName());
	}

	@Override
	public synchronized void start() {
		super.start();
		counter.start();
	}

	@Override
	public synchronized void stop() {
		producer.close();
		counter.stop();
		super.stop();
	}
}
