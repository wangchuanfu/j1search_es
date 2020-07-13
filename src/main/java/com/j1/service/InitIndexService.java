package com.j1.service;

import com.j1.pojo.Content;
import com.j1.utils.IntoEsUtils;
import com.j1.utils.JsoupUtils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import org.elasticsearch.action.search.SearchRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangchuanfu on 20/7/10.
 */
@Service
@Slf4j
public class InitIndexService {

    @Resource
    JsoupUtils jsoupUtils;
    @Resource
    IntoEsUtils intoEsUtils;
    @Resource
    @Qualifier("restHighLevelClient")
    public RestHighLevelClient client;

    public boolean initIndex(String keyWord) throws Exception {
        //从京东爬取数据
        ArrayList<Content> goodsList = jsoupUtils.getJdDataByJsoup(keyWord);
        //推送到es中
        intoEsUtils.insertEsByBulk("jd_goods", "_doc", goodsList, "sku");

        return true;
    }


    public List<Map<String, Object>> search(String keyword, Integer pageNo, Integer pageSize) throws Exception {
        return querySearch(keyword, pageNo, pageSize);
    }


    //根据关键字查询
    public List<Map<String, Object>> querySearch(String keyword, Integer pageNo, Integer pageSize) throws Exception {

        //创建searchRequest
        SearchRequest searchRequest = new SearchRequest("jd_goods");
        //构建查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);//term 精确查找
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", keyword);//一般match查询
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(10));//查询时间
        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.from(pageNo);
        //显示高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");

        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);

        log.info(searchRequest.toString());
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //循环遍历
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();//查询的原来的结果
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            if (title != null) {
                //解析高亮字段,将之前查出来的没高亮的字段 替换为高亮字段
                Text[] fragments = title.fragments();
                StringBuilder newTitle = new StringBuilder();
                for (Text fragment : fragments) {
                    newTitle.append(fragment.string());
                }
                sourceAsMap.put("title", newTitle);
            }
            list.add(sourceAsMap);

        }
        return list;

    }
}
