package com.j1.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * Created by wangchuanfu on 20/7/9.
 */
@Configuration

public class ElasticsearchClientConfig {

    @Bean
    public RestHighLevelClient restHighLevelClient(){

        RestHighLevelClient client = new RestHighLevelClient(

                RestClient.builder(new HttpHost("localhost",9200,"http")));
        return client;
    }

}
