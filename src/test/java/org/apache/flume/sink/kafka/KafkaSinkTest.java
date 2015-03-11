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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.sink.AbstractSink;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

public class KafkaSinkTest {

	private KafkaDynamicTopicSink kafkaSink;
	private Producer<byte[], byte[]> mockProducer;
	private Channel mockChannel;
	private Event mockEvent;
	private Transaction mockTx;
    private KafkaDynamicSinkCounter kafkaSinkCounter;

	@SuppressWarnings("unchecked")
	@Before
	public void setup() throws Exception {
		mockProducer = mock(Producer.class);
		mockChannel = mock(Channel.class);
		mockEvent = mock(Event.class);
		mockTx = mock(Transaction.class);
		kafkaSink = new KafkaDynamicTopicSink();
        kafkaSinkCounter = new KafkaDynamicSinkCounter("testSinkCounter");
		
		Field field = AbstractSink.class.getDeclaredField("channel");
		field.setAccessible(true);
		field.set(kafkaSink, mockChannel);

		field = KafkaDynamicTopicSink.class.getDeclaredField("defaultTopic");
		field.setAccessible(true);
		field.set(kafkaSink, "defaultTopic");

		field = KafkaDynamicTopicSink.class.getDeclaredField("producer");
		field.setAccessible(true);
		field.set(kafkaSink, mockProducer);

        field = KafkaDynamicTopicSink.class.getDeclaredField("counter");
        field.setAccessible(true);
        field.set(kafkaSink, kafkaSinkCounter);
		
		when(mockChannel.take()).thenReturn(mockEvent);
		when(mockChannel.getTransaction()).thenReturn(mockTx);
        doNothing().when(mockProducer).send(Matchers.<KeyedMessage<byte[], byte[]>>any());
    }

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStatusReady() throws EventDeliveryException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		when(mockEvent.getBody()).thenReturn("frank".getBytes());

        Status status = kafkaSink.process();
		verify(mockChannel, times(1)).getTransaction();
		verify(mockChannel, times(1)).take();
		verify(mockProducer, times(1)).send((KeyedMessage<byte[], byte[]>) any());
		verify(mockTx, times(1)).commit();
		verify(mockTx, times(0)).rollback();
		verify(mockTx, times(1)).close();
        assertEquals(0, kafkaSinkCounter.getCounterMessageSentError());
        assertEquals(1, kafkaSinkCounter.getCounterMessageSent());
		assertEquals(Status.READY, status);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStatusBackoff() throws EventDeliveryException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		when(mockEvent.getBody()).thenThrow(new RuntimeException());
		Status status = kafkaSink.process();
		verify(mockChannel, times(1)).getTransaction();
		verify(mockChannel, times(1)).take();
		verify(mockProducer, times(0)).send((KeyedMessage<byte[], byte[]>) any());
		verify(mockTx, times(0)).commit();
		verify(mockTx, times(1)).rollback();
		verify(mockTx, times(1)).close();
        assertEquals(1, kafkaSinkCounter.getCounterMessageSentError());
        assertEquals(0, kafkaSinkCounter.getCounterMessageSent());
		assertEquals(Status.BACKOFF, status);
	}

    @SuppressWarnings("unchecked")
    @Test
    public void testFailChannelTake() throws EventDeliveryException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        when(mockEvent.getBody()).thenReturn("frank".getBytes());
        doThrow(new RuntimeException()).when(mockChannel).take();

        Status status = kafkaSink.process();
        verify(mockChannel, times(1)).getTransaction();
        verify(mockChannel, times(1)).take();
        verify(mockProducer, times(0)).send((KeyedMessage<byte[], byte[]>) any());
        verify(mockTx, times(0)).commit();
        verify(mockTx, times(1)).rollback();
        verify(mockTx, times(1)).close();
        assertEquals(0, kafkaSinkCounter.getCounterMessageSentError());
        assertEquals(0, kafkaSinkCounter.getCounterMessageSent());
        assertEquals(Status.BACKOFF, status);
    }

    @Test
    public void testKafkaSendFailure() throws EventDeliveryException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        //when(mockProducer.send((KeyedMessage<byte[], byte[]>) any())).thenThrow(new RuntimeException());
        doThrow(new RuntimeException()).when(mockProducer).send(Matchers.<KeyedMessage<byte[], byte[]>>any());

        Status status = kafkaSink.process();
        verify(mockChannel, times(1)).getTransaction();
        verify(mockChannel, times(1)).take();
        verify(mockProducer, times(1)).send(Matchers.<KeyedMessage<byte[], byte[]>>any());
        verify(mockTx, times(0)).commit();
        verify(mockTx, times(1)).rollback();
        verify(mockTx, times(1)).close();
        assertEquals(1, kafkaSinkCounter.getCounterMessageSentError());
        assertEquals(0, kafkaSinkCounter.getCounterMessageSent());
        assertEquals(Status.BACKOFF, status);
    }
}
