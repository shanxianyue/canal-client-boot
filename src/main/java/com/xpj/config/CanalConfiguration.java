package com.xpj.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

@Configuration
public class CanalConfiguration {

    @Value("${canal.destination}")
    private String destination;

    @Value("${canal.port}")
    private int port;

    @Bean
    public CanalConnector canalConnector(){
        return CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(), port),
                destination,
                "",
                "");
    }



}
