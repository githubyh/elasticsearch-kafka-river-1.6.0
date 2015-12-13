package com.silicon.plugin.river.kafka.module;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

import com.silicon.plugin.river.kafka.KafkaRiver;

public class KafkaRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(KafkaRiver.class).asEagerSingleton();
		
	}

}
