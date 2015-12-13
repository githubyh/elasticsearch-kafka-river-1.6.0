package com.silicon.plugin.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.silicon.plugin.kafka.Util.Utils;
import com.silicon.plugin.kafka.task.ConsumerTask;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer implements Runnable {
	private final static ESLogger logger = ESLoggerFactory.getLogger(KafkaConsumer.class.getName());

	private Properties properties;
	private Client client;
	private ExecutorService executor;
	private ConsumerConnector connector = null;
	
	public KafkaConsumer(Client client, Properties properties) {
		this.client = client;
		this.properties = properties;
	}

	@Override
	public void run() {
		ConsumerConfig config = new ConsumerConfig(this.properties);
		this.connector = Consumer.createJavaConsumerConnector(config);
		this.executor = Executors.newFixedThreadPool(1);
		String topics = properties.getProperty(Utils.ZK_TOPIC);
		String[] allTopics = Strings.splitStringByCommaToArray(topics);
		if (allTopics.length == 0) {
			logger.info("## topic interested is empty, return ");
			return;
		}
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		for (int i = 0; i < allTopics.length; i++) {
			String topic = allTopics[i];
			topicMap.put(topic, Utils.STREAM_NUMBER); // default one stream for each topic;
		}
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicMap);
		Set<String> keys = consumerMap.keySet();
		for (String key : keys) {
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(key);
			int threadNum = 0;
			for (KafkaStream<byte[], byte[]> stream : streams) {
				executor.submit(new ConsumerTask(stream, threadNum, key, client));
				threadNum++;
			}
		}

	}
	
	public void shutdown(){
		if(connector != null){
			connector.shutdown();
		}
		if(executor != null){
			executor.shutdown();
		}
		if(client != null){
			client.close();
		}
	}
	
}
