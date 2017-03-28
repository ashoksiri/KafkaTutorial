package com.ashok.kafka.test.cases;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class WriteFileFromKafka extends Thread{
	/**
	 * The topic you want to get the data
	 */
	private final static String TOPIC = "hadoop";
	private ConsumerConnector consumerConnector;

	/*
	 * Main Method the Execution starts from
	 */
	public static void main(String[] argv) throws UnsupportedEncodingException {
		WriteFileFromKafka consumer = new WriteFileFromKafka();
		consumer.start();
	}

	/**
	 * Creating Kafka Consumer Connector usign Properties
	 */
	public WriteFileFromKafka() {
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
			{
				String message = new String(it.next().message());
				System.out.println(message);
				writeToFile(message);
			}

	}
	
	public void writeToFile(String line){
		System.out.println("---------------------------Inside writing File ------------------------------------------");
		File file = new File("/home/user/kafkafile.txt");
		
		BufferedWriter writer =  null; 
		try{
			
			writer = new BufferedWriter(new FileWriter(file,true));
			writer.write(line+"\t"+new Date()+"\n");
			writer.flush();
		}
		catch(IOException e)
		{
			
			e.printStackTrace();
		}
		finally{
			if(writer!=null)
			{
				try {
					writer.close();
				} catch (IOException e) {
					
					e.printStackTrace();
				}
			}
			
		}
		
	}
}
