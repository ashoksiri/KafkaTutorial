package com.ashok.kafka.test;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 
 * @author ashok
 *
 */
public class KafkaConsumer extends Thread {

	/**
	 * The topic you want to get the data
	 */
	private final static String TOPIC = "hadoop";
	private ConsumerConnector consumerConnector;

	/*
	 * Main Method the Execution starts from
	 */
	public static void main(String[] argv) throws UnsupportedEncodingException {
		KafkaConsumer consumer = new KafkaConsumer();
		consumer.start();
	}

	/**
	 * Creating Kafka Consumer Connector usign Properties
	 */
	public KafkaConsumer() {
		Properties properties = new Properties();
		// properties.put("zookeeper.connect","10.25.3.214:2181");
		properties.put("zookeeper.connect", "localhost:2181");
		properties.put("group.id", "test-group");
		ConsumerConfig consumerConfig = new ConsumerConfig(properties);
		consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

	}

	@Override
	public void run() {
		System.out.println("insid run");
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext())
			System.out.println(new String(it.next().message()));

	}

}