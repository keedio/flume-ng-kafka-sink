package org.apache.flume.sink.demo.kafka;

public interface KafkaSinkCounterMBean {

	public void increaseCounterMessageSent();

	public void increaseCounterMessageSentError();

	public long getCounterMessageSent();

	public long getCounterMessageSentError();

	public long getCurrentThroughput();

	public long getAverageThroughput();

}
