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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.interceptor.EnrichedEventBody;

public class KafkaDynamicSinkUtil {
	private static final Logger log = LoggerFactory.getLogger(KafkaDynamicSinkUtil.class);
	
	private static final String FILTER_REGEX="%[^%]+%";

	public static Properties getKafkaConfigProperties(Context context) {
		log.info("context={}",context.toString());
		Properties props = new Properties();
		Map<String, String> contextMap = context.getParameters();
		for(String key : contextMap.keySet()) {
			if (!key.equals("type") && !key.equals("channel") && !key.equals("defaultTopic") 
					&& !key.equals("dynamicTopic")) {
				props.setProperty(key, context.getString(key));
				log.info("key={},value={}", key, context.getString(key));
			}
		}
		return props;
	}
	public static Producer<byte[], byte[]> getProducer(Context context) {
		Producer<byte[], byte[]> producer;
		producer = new Producer<byte[], byte[]>(new ProducerConfig(getKafkaConfigProperties(context)));
		return producer;
	}
	
	/**
	 * Returns the topic destination String using dynamic topic property
	 * @param dynamicTopic Configuration dynamic topic property, all keys to build the destination
	 * topic String must be between $, e.g "%key1%-%key2%" will return value1-value2 
	 * @param eventBody Body with the extra fields specified in dynamic topic String
	 * @return Destination topic, if no pattern match null is returned
	 */
	public static String getDestinationTopic(String dynamicTopic, String defaultTopic, byte[] eventBody){
		if (dynamicTopic == null){
			log.debug("DynamicTopic not configured, sending message to default topic: {}", defaultTopic);
			return defaultTopic;
		}
		try{
			Map<String,String> extraData = EnrichedEventBody.createFromEventBody(eventBody, true).getExtraData();
			
			// GET DYNAMIC TOPIC
			List<String> keys = getDynamicTopicKeys(dynamicTopic);
			
			int i=0;
			while ( i<keys.size() && extraData.containsKey(keys.get(i))){
				i++;
			}
			// if all keys are found in extraData, the destination topic is build		
			if (i==keys.size()){
				String destinationTopic = dynamicTopic;
				destinationTopic = destinationTopic.replaceAll("%", "");
				for (i=0;i<keys.size();i++){
					destinationTopic = destinationTopic.replace(keys.get(i), extraData.get(keys.get(i)));
				}
				
				// Substitute all blank spaces with _
				destinationTopic = destinationTopic.replaceAll(" ", "_");
				log.warn("Dynamic destination topic with pattern {}: {}", dynamicTopic, destinationTopic);
				return destinationTopic;
			}
			// if all keys are not found in extra data the default topic is returned 
			else{
				log.warn("Keys: {} not found in extra data, sending to default topic: {}", keys, defaultTopic);
				return defaultTopic;
			}
		}catch (IOException e){
			log.warn("Extra data corruption in message, sending message to default topic: {}", defaultTopic);
			return defaultTopic;
		}
	}
	
	/**
	 * Returns a List of the keys used to build the destination topic
	 * @param dynamicTopic The configuration String from properties file 
	 * @return A List of the keys to build the destination Topic 
	 */
	private static List<String> getDynamicTopicKeys(String dynamicTopic){
		Pattern pattern = Pattern.compile(FILTER_REGEX);
		log.debug("REGEX: " + FILTER_REGEX);
		Matcher matcher = pattern.matcher(dynamicTopic);
		List<String> topicKeys = new ArrayList<String>();
		
		int i=0;
		while (matcher.find()){
				topicKeys.add(matcher.group(i).replace("%", ""));
		}
		
		return topicKeys;
	}
}


















