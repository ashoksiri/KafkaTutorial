package com.ashok.kafka.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaTopics 
{
	public static void main(String[] args) 
	
	{
		SimpleConsumer consumer = new SimpleConsumer("MSR", 9092, 10000, 100000, "test");
		TopicMetadataResponse response = consumer.send(new TopicMetadataRequest(new ArrayList<String>()));
		
		List<TopicMetadata> topicMetadata = response.topicsMetadata();
		
		Iterator<TopicMetadata> metadata = topicMetadata.iterator();
		while(metadata.hasNext())
		{
			System.out.println(metadata.next().topic());
		}
	}
}
