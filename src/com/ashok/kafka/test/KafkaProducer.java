package com.ashok.kafka.test;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer 
{
	private static Producer<Integer, String> producer;
	private final Properties props = new Properties();
		public KafkaProducer()
		{
			props.put("metadata.broker.list", "10.1.7.49:9092");
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			producer = new Producer<Integer, String>(new ProducerConfig(props));
		}
	
	public static void main(String[] args) 
	{
		new KafkaProducer();
		String topic = "hadoop";
		String messageStr = "Welcome to Kafka World";
		KeyedMessage<Integer, String> data =
		new KeyedMessage<Integer, String>(topic, messageStr);
		producer.send(data);
		producer.close();
		System.out.println("Message Sent Successfully");
	}
}
