package com.ashok.kafka.test.cases;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ReadFileToKafka {

	private static Producer<Integer, String> producer;
	private final Properties props = new Properties();

	public ReadFileToKafka() {
		props.put("metadata.broker.list", "10.1.7.49:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}

	static KeyedMessage<Integer, String> data = null;
	static BufferedReader reader = null;

	public static void main(String[] args) throws IOException {
		new ReadFileToKafka();
		String topic = "hadoop";

		try {
			File file = new File("/home/user/sample.txt");
			reader = new BufferedReader(new FileReader(file));
			String messageStr = "";

			while ((messageStr = reader.readLine()) != null) {
				data = new KeyedMessage<Integer, String>(topic, messageStr);
				producer.send(data);
			}

			producer.close();

			System.out.println("Message Sent Successfully");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				reader.close();
			}
		}

	}
}
