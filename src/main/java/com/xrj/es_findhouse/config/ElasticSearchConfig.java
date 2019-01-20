package com.xrj.es_findhouse.config;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {

    @Value("${elasticsearch.host}")
    private String esHost;

    @Value("${elasticsearch.port}")
    private int esPort;

    @Value("${elasticsearch.cluster.name}")
    private String esName;
    
    @Bean
    public TransportClient transportClient() throws UnknownHostException {
    	Settings settings = Settings.builder()
//    			.put("cluster.name", esName)
    			.put("client.transport.sniff", true)
    			.build();
    	
		TransportClient client = new PreBuiltTransportClient(settings)
    			.addTransportAddress(new TransportAddress(InetAddress.getByName(esHost), esPort));
    	return client;
    }
	
}
