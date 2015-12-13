package com.silicon.plugin.kafka.Util;

import java.util.Properties;

public class Utils {
	public static String KAFKA = "kafka";
	
	public static String ZK_HOST = "zookeeper.connect";
	public static String KAFKA_ZK_LOCALHOST = "localhost:2181";
	
	public static String ZK_GROUPID = "group.id";
	public static String KAFKA_ZK_GROUPID = "test-group";
	
	public static String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout.ms";
	public static int KAFKA_ZK_SESSION_TIMEOUT = 400;
	
	public static String ZK_SYNC_TIME = "zookeeper.sync.time.ms";
	public static int KAFKA_ZK_SYNC_TIME = 200;
	
	public static String ZK_COMMIT_INTEVAL = "auto.commit.interval.ms";
	public static int KAFKA_ZK_COMMIT_INTEVAL = 100;
	
	public static String ZK_TOPIC = "kafka.topic";
	public static String KAFKA_ZK_TOPIC = "test";

	public static String LINE_SEPARATOR = "\r\n";
	public static Integer STREAM_NUMBER = 1;

	public static String PREFIX_INDEX = "_index";
	public static String PREFIX_TYPE = "_type";
	public static String PREFIX_ID = "_id";
	public static String PREFIX_ACTION = "_action";

	public static String ACTION_INSERT = "insert";
	public static String ACTION_UPDATE = "update";
	public static String ACTION_DELETE = "delete";

	public static Properties properties = null;

}
