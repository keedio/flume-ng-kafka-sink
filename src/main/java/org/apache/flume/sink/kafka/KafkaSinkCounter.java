package org.apache.flume.sink.kafka;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class KafkaSinkCounter extends MonitoredCounterGroup implements KafkaSinkCounterMBean{

	private static long counter_message_sent;
	private static long counter_message_sent_error;
	private static long last_sent;
	private static long start_time;
	
	
	private static final String[] ATTRIBUTES = {
		"counter_message_sent","counter_message_sent_error","last_sent","start_time"
	  };
	
	public KafkaSinkCounter(String name) {
		super(MonitoredCounterGroup.Type.SINK, name, ATTRIBUTES);
		counter_message_sent = 0;
		counter_message_sent_error = 0;
		last_sent = 0;
		setStartTime();
	}
	
	public long increaseCounterMessageSent(){
		last_sent = System.currentTimeMillis();
		return counter_message_sent++;
	}
	
	public long getCounterMessageSent(){
		return counter_message_sent;
	}

	public long getLastSent() {
		return last_sent;
	}
	
	public long setStartTime(){
		start_time = System.currentTimeMillis();
		return start_time;
	}
	
	public long getStartTime(){
		return start_time;
	}

	public long increaseCounterMessageSentError() {
		return counter_message_sent_error++;
	}

	public long getCounterMessageSentError() {
		return counter_message_sent_error;
	}
}
