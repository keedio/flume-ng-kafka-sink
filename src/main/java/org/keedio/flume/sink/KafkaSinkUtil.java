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
package org.keedio.flume.sink;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.flume.Context;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSinkUtil {
	private static final Logger log = LoggerFactory.getLogger(KafkaSinkUtil.class);

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
	
	//public static String getDestinationTopic(ZkClient zkClient, String dynamicTopic,String defaultTopic, byte[] eventBody){
	public static String getDestinationTopic(String dynamicTopic, String defaultTopic, byte[] eventBody){
		if (dynamicTopic == null){
			log.debug("DynamicTopic not configured, sending message to default topic: {}", defaultTopic);
			return defaultTopic;
		}
		
		try{
			Map<String,String> extraData = EnrichedEventBody.createFromEventBody(eventBody, true).getExtraData();
			String[] keys = dynamicTopic.split("-");
			
			for (int j=0;j<keys.length;j++)
				log.debug("KEY "+ j+ " : " + keys[j]);
			
			int i=0;
			while ( i<keys.length && extraData.containsKey(keys[i])){
				i++;
			}
			
			// if all keys are found in extraData, the destination topic is build
			String destinationTopic=null;
			if (i==keys.length){
				destinationTopic = new String();
				for (i=0;i<keys.length;i++){
					destinationTopic += extraData.get(keys[i]).replace(" ", "_");
					if (i!=keys.length-1)
						destinationTopic += "-";
				}
			}
			else{
				destinationTopic=defaultTopic;
			}
				
    		return destinationTopic;
		}catch (IOException e){
			log.warn("Extra data corruption in message, sending message to default topic: {}", defaultTopic);
			return defaultTopic;
		}
	}
}


















