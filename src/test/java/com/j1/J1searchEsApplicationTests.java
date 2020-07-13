package com.j1;

import com.alibaba.fastjson.JSON;
import com.j1.pojo.Content;
import com.j1.service.InitIndexService;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import javax.annotation.security.RunAs;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

//@RunWith(SpringRunner.class)  如果项目运行报错,pom里引入junit4 的包
@SpringBootTest(classes = J1searchEsApplication.class)
class J1searchEsApplicationTests {


	@Autowired
	@Qualifier("restHighLevelClient")
	public RestHighLevelClient client;
	@Resource
	InitIndexService initIndexService;
	//创建索引
	@Test
	public void testCreatIndex() throws Exception {
		//创建索引请求
		CreateIndexRequest request = new CreateIndexRequest ( "goods" );
		//执行请求
		CreateIndexResponse response = client.indices ().create ( request, RequestOptions.DEFAULT );
		System.out.print ( response.toString () );

	}

	//判断索引是否存在
	@Test
	public void testExistsIndex() throws Exception {
		//创建索引请求
		GetIndexRequest request = new GetIndexRequest ( "books" );
		boolean exists = client.indices ().exists ( request, RequestOptions.DEFAULT );//
		System.out.println ( exists );

	}
	//添加document


	@Test
	public void testAdddocument() throws Exception {
		//创建索引请求
		Content content = new Content ( "12-44-87","java","https://68836f52ffaaad96.jpg", "https://68836f52ffaaad96.jpg", "12","新华书店" );
		IndexRequest request = new IndexRequest ( "books" );
		request.id ( "1" );
		request.timeout ( "1s" );
		//封装成json 数据
		request.source ( JSON.toJSONString ( content ), XContentType.JSON );
		IndexResponse indexResponse = client.index ( request, RequestOptions.DEFAULT );
		System.out.print ( indexResponse.status () );
	}

	// 更新文档的信息
	@Test
	public void updateDocument() throws Exception {
		UpdateRequest request = new UpdateRequest ( "books", "EZy1MnMBHRmjGcMetQhK" );
		request.timeout ( "1s" );
		Content content = new Content ( "12-44-87","java","https://68836f52ffaaad96.jpg", "https://68836f52ffaaad96.jpg", "12","新华书店" );
		request.doc ( JSON.toJSONString ( content ), XContentType.JSON );
		UpdateResponse updateResponse = client.update ( request, RequestOptions.DEFAULT );
		System.out.print ( updateResponse.status () );

	}

	// 删除文档记录
	@Test
	public void testDeleteRequest() throws Exception {
		DeleteRequest request = new DeleteRequest ( "books", "EZy1MnMBHRmjGcMetQhK" );
		request.timeout ( "1s" );
		DeleteResponse deleteResponse = client.delete ( request,
				RequestOptions.DEFAULT );
		System.out.println ( deleteResponse.status () );
	}

	//批量插入数据


	@Test
	public void testAddBulkRequest() throws Exception {
		BulkRequest request = new BulkRequest ();
		request.timeout ( "3s" );
		ArrayList <Content> contentList = new ArrayList ();
		for (int i = 0; i < 4; i++) {
			contentList.add (  new Content ( "12-44-87","java","https://68836f52ffaaad96.jpg", "https://68836f52ffaaad96.jpg", "12","新华书店" ));
		}
		for (int i = 0; i < contentList.size (); i++) {
			request.add (
					new IndexRequest ( "books" )
							.id ( "" + (i + 1) )
							.source ( JSON.toJSONString ( contentList.get ( i ) ), XContentType.JSON ) );
			BulkResponse responses = client.bulk ( request, RequestOptions.DEFAULT );
			System.out.println ( responses.hasFailures () );
		}


	}


	// 查询
// SearchRequest 搜索请求
// SearchSourceBuilder 条件构造
// HighlightBuilder 构建高亮
// TermQueryBuilder 精确查询
// MatchAllQueryBuilder
// xxx QueryBuilder 对应我们刚才看到的命令!

	/**
	 *
	 * GET /books/_search
	 {
	 "query": {
	 "term": {
	 "title": {
	 "value": "elasticsearch"
	 }
	 }
	 }
	 }
	 * @throws Exception
	 */


	// searchRequest 发送请求,sourceBuilder 条件构造,QueryBuilders 用来构造具体查询条件(精确,全部,前缀等等)
	//client --searchRequest--sourceBuilder--query--termQueryBuilder
	@Test
	public void testSearch() throws Exception {
		SearchRequest searchRequest = new SearchRequest ( "books" );
// 构建搜索条件
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder ();
		sourceBuilder.highlighter ();
// 查询条件,我们可以使用 QueryBuilders 工具来实现

		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery ( "title",
				"elasticsearch" );
		MatchAllQueryBuilder allQueryBuilder = QueryBuilders.matchAllQuery ();
		sourceBuilder.query ( termQueryBuilder )
				.query ( allQueryBuilder );

		sourceBuilder.timeout ( new TimeValue ( 60, TimeUnit.SECONDS ) );
		searchRequest.source ( sourceBuilder );
		SearchResponse searchResponse = client.search ( searchRequest,
				RequestOptions.DEFAULT );


		//打印DSL
		System.out.println (searchRequest.toString() );

		System.out.println ( JSON.toJSONString ( searchResponse.getHits () ) );
		System.out.println ( "=================================" );
		for (SearchHit documentFields : searchResponse.getHits ().getHits ()) {
			System.out.println ( documentFields.getSourceAsMap () );
		}

	}
	//将数据推送到es中
	@Test
	public  void insertDataToEs() throws Exception{
		boolean b = initIndexService.initIndex ( "java" );
		System.out.println ( b );
	}
}
