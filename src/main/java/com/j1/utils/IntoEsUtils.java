package com.j1.utils;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by wangchuanfu on 20/7/10.
 */
@Slf4j
@Component
public class IntoEsUtils {


    @Resource
    @Qualifier("restHighLevelClient")
    public RestHighLevelClient client;

    //批量插入数据到es中
    public boolean insertEsByBulk(String indexName, String type, List <?> dataList, String idFieldName) throws Exception {
        try {
            //先判断索引是否存在
            if(!existsIndex(indexName)){
                creatIndex(indexName);
            }
            BulkRequest bulkRequest = new BulkRequest();
            dataList.forEach((data) -> {
                UpdateRequest request = this.getUpdateRequest(indexName, type);
                Object id = null;
                try {
                    id = FieldUtils.readField(data, idFieldName, true);
                } catch (Exception e) {
                    e.printStackTrace ();
                    log.error("{} 获取id失败", id, e);
                }
                request.id(String.valueOf(id));
                request.doc(JSON.toJSONString(data), XContentType.JSON);
                request.upsert(JSON.toJSONString(data), XContentType.JSON);
                bulkRequest.add(request);
            });
            this.client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("{} 批量插入失败", indexName, e);
        }
        return true;
    }

    private UpdateRequest getUpdateRequest(String indexName, String type) {
        UpdateRequest request = new UpdateRequest ();
        request.index (indexName);
        if (StringUtils.isNotBlank(type)) {
            //7.x之后type 可以不指定,默认为_doc
          request.type(type);
        }

        return request;

    }

    //判断index是否存在
   public boolean existsIndex(String indexName) throws  Exception{
       GetIndexRequest request = new GetIndexRequest ( indexName );
       boolean exists = client.indices ().exists ( request, RequestOptions.DEFAULT );
        return exists;
   }

    //创建index
    public  boolean creatIndex(String indexName) throws Exception {
        //这里只是创建索引,并没有创建mapping,
        //创建索引请求
        CreateIndexRequest request = new CreateIndexRequest ( indexName );
        //执行请求
       client.indices ().create ( request, RequestOptions.DEFAULT );
        return true;
    }

}
