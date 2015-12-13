package com.silicon.plugin.river.kafka;

import java.util.Map;
import java.util.Properties;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import com.silicon.plugin.kafka.Util.Utils;
import com.silicon.plugin.kafka.consumer.KafkaConsumer;

public class KafkaRiver extends AbstractRiverComponent implements River {
	private final static ESLogger logger = ESLoggerFactory.getLogger(KafkaRiver.class.getName());

	private Client client;
	private Thread thread;
	private String zookeeperHost = null;
	private String groupId = null;
	private int sessionTimeoutInteval = 0;
	private int syncTime = 0;
	private int commitTimeInteval = 0;
	private String topics = null;

	@SuppressWarnings({ "unchecked" })
	@Inject
	protected KafkaRiver(RiverName riverName, RiverSettings settings, Client client) {
		super(riverName, settings);
		this.client = client;

		Map<String, Object> setting = settings.settings();

		if (setting.containsKey(Utils.KAFKA)) {
			Map<String, Object> kafkaSetting = (Map<String, Object>) setting.get(Utils.KAFKA);
			this.zookeeperHost = (String) kafkaSetting.get(Utils.ZK_HOST);
			if (this.zookeeperHost == null) {
				this.zookeeperHost = Utils.KAFKA_ZK_LOCALHOST;
			}

			this.groupId = (String) kafkaSetting.get("group.id");
			if (this.groupId == null) {
				this.groupId = Utils.KAFKA_ZK_GROUPID;
			}

			this.sessionTimeoutInteval = (int) kafkaSetting.get(Utils.ZK_SESSION_TIMEOUT);
			if (this.sessionTimeoutInteval <= 0) {
				this.sessionTimeoutInteval = Utils.KAFKA_ZK_SESSION_TIMEOUT;
			}

			this.syncTime = (int) kafkaSetting.get(Utils.ZK_SYNC_TIME);
			if (this.syncTime <= 0) {
				this.syncTime = Utils.KAFKA_ZK_SYNC_TIME;
			}

			this.commitTimeInteval = (int) kafkaSetting.get(Utils.ZK_COMMIT_INTEVAL);
			if (this.commitTimeInteval <= 0) {
				this.commitTimeInteval = Utils.KAFKA_ZK_COMMIT_INTEVAL;
			}

			this.topics = (String) kafkaSetting.get(Utils.ZK_TOPIC);
			if (this.topics == null) {
				this.topics = Utils.KAFKA_ZK_TOPIC;
			}

		} else {
			this.zookeeperHost = Utils.KAFKA_ZK_LOCALHOST;
			this.groupId = Utils.KAFKA_ZK_GROUPID;
			this.sessionTimeoutInteval = Utils.KAFKA_ZK_SESSION_TIMEOUT;
			this.syncTime = Utils.KAFKA_ZK_SYNC_TIME;
			this.commitTimeInteval = Utils.KAFKA_ZK_SYNC_TIME;
			this.topics = Utils.KAFKA_ZK_TOPIC;
		}
	}

	@Override
	public void start() {
		Properties props = new Properties();
		props.put(Utils.ZK_HOST, this.zookeeperHost);
		props.put(Utils.ZK_GROUPID, this.groupId);
		props.put(Utils.ZK_SESSION_TIMEOUT, this.sessionTimeoutInteval);
		props.put(Utils.ZK_SYNC_TIME, this.syncTime);
		props.put(Utils.ZK_COMMIT_INTEVAL, this.commitTimeInteval);
		props.put(Utils.ZK_TOPIC, this.topics);
		thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kafka_river")
				.newThread(new KafkaConsumer(client, props));
		thread.start();
	}

	@Override
	public void close() {
		thread.interrupt();
	}

}
