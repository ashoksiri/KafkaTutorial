package com.msr.kafka.topics;

import org.I0Itec.zkclient.ZkClient;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.Seq;

public class CreateTopic {

	public static void main(String[] args) {
		
		String zookeeperConnect = "localhost:2181";
		int sessionTimeoutMs = 10*1000;
		int connectionTimeoutMs = 8*1000;
		
		ZkClient zkClient1 = new ZkClient(
		        zookeeperConnect,
		        sessionTimeoutMs,
		        connectionTimeoutMs,
		        ZKStringSerializer$.MODULE$);

		    Seq<String> util = ZkUtils.getAllTopics(zkClient1).toList();
		    System.out.println(util);
		    
		    String topic = "testing";
		    
		    if(!util.contains(topic))
		    {
		    	AdminUtils.createTopic(zkClient1, topic, 2, 1, new java.util.Properties());
		    }
		    
		    //AdminUtils.deleteTopic(zkClient1, "test");
	}
}
