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

import java.util.ArrayList;
import java.util.HashMap;
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

public class KafkaSinkUtil {
	private static final Logger log = LoggerFactory.getLogger(KafkaSinkUtil.class);
	
	//TODO: Delete this after Emmanuelle class has been added
	private static int fakeMessageID=0;
	
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
	public static String getDestinationTopic(String dynamicTopic, byte[] eventBody){
		//Map<String,String> extraData = eventBody.getExtraData();
		
		// TODO: Change this harcoded source with Emmanuelle getExtraData() method
		Map<String,String> extraData = new HashMap<String,String>();
		
		extraData.put("CLIENT", "Client-" + fakeMessageID);
		extraData.put("VDC_NAME", "VdcName-" + fakeMessageID);
		extraData.put("PRODUC_TYPE", "ProductType-" + fakeMessageID);
		
		fakeMessageID = (fakeMessageID + 1) % 2;
		// END harcoded source
		
		// GET DYNAMIC TOPIC
		List<String> keys = getDynamicTopicKeys(dynamicTopic);
		
		int i=0;
		while (extraData.containsKey(keys) && i<keys.size()){
			i++;
		}
		
		// if all keys are found in extraData, the destination topic is build
		if (i==keys.size()){
			String destinationTopic = dynamicTopic;
			for (i=0;i<keys.size();i++){
				destinationTopic.replaceFirst(FILTER_REGEX, extraData.get(keys.get(i)));
				
			}
			log.debug("Dynamic destination topic for " + dynamicTopic + ": " + destinationTopic);
			return destinationTopic;
		}
		
		
		return null;
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
		
		log.debug("dynamicTopic: " + dynamicTopic);
		log.debug("Count: " + matcher.groupCount());
		log.debug("Matcher: " + matcher.toString());
		log.debug("pattern: " + pattern.toString());
		
		for (int i=0;i<matcher.groupCount();i++){
		    topicKeys.add(matcher.group(i).replace("%", ""));
		}
		
		log.debug("MARCELO -----> KEYS: " + topicKeys.toString());
		
		return topicKeys;
	}
}

















