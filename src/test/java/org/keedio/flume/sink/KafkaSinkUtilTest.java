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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Properties;

import org.apache.flume.Context;
import org.keedio.flume.sink.KafkaSinkUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaSinkUtilTest {
	private Properties props = new Properties();

	@Before
	public void setUp() throws Exception {
		Context context = new Context();
		context.put("consumer.timeout", "10");
		context.put("type", "KafkaSource");
		context.put("defaultTopic", "defaultTopic");
		context.put("dynamicTopic", "$KEY$");
		props = KafkaSinkUtil.getKafkaConfigProperties(context);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetKafkaConfigParameter() {
		assertEquals("10",props.getProperty("consumer.timeout"));
		assertNull(props.getProperty("defaultTopic"));
		assertNull(props.getProperty("type"));
		assertNull(props.getProperty("dynamicTopic"));
	}

	@Test
	public void testGetProducer() {
//		fail("Not yet implemented");
	}

	@Test
	public void testGetConsumer() {
//		fail("Not yet implemented");
	}

}
