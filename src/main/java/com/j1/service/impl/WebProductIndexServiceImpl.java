package com.j1.service.impl;
/**
 * import com.alibaba.fastjson.JSON;
 * import com.huayuan.search.common.DefaultParams;
 * import com.huayuan.search.common.type.DefaultIndexField;
 * import com.huayuan.search.common.type.ProductFieldEnum;
 * import com.huayuan.search.core.annotation.IndexField;
 * import com.huayuan.search.core.service.impl.AbstractEsService;
 * import com.huayuan.search.utils.DateUtils;
 * import com.huayuan.search.web.model.WebAbroadProductVo;
 * import com.huayuan.search.web.service.CommonDBService;
 * import com.huayuan.search.web.service.WebAbroadProductIndexService;
 * import com.huayuan.search.web.service.WebProductIndexService;
 * import com.j1.base.type.NewProductFieldEnum;
 * import com.j1.promote.model.service.PromoteProductService;
 * import org.apache.commons.lang.StringUtils;
 * import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
 * import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
 * import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
 * import org.elasticsearch.action.bulk.BulkRequestBuilder;
 * import org.elasticsearch.action.bulk.BulkResponse;
 * import org.elasticsearch.action.delete.DeleteResponse;
 * import org.elasticsearch.action.get.GetResponse;
 * import org.elasticsearch.action.index.IndexRequestBuilder;
 * import org.elasticsearch.action.index.IndexResponse;
 * import org.elasticsearch.action.update.UpdateRequestBuilder;
 * import org.elasticsearch.action.update.UpdateResponse;
 * import org.elasticsearch.client.Client;
 * import org.elasticsearch.client.Requests;
 * import org.elasticsearch.cluster.ClusterState;
 * import org.elasticsearch.cluster.metadata.IndexMetaData;
 * import org.elasticsearch.cluster.metadata.MappingMetaData;
 * import org.elasticsearch.common.xcontent.XContentBuilder;
 * import org.elasticsearch.common.xcontent.XContentFactory;
 * import org.slf4j.Logger;
 * import org.slf4j.LoggerFactory;
 * import org.springframework.beans.factory.annotation.Autowired;
 * import org.springframework.stereotype.Service;
 * <p>
 * import java.io.IOException;
 * import java.util.ArrayList;
 * import java.util.Date;
 * import java.util.List;
 * import java.util.Map;
 * <p>
 * import static com.huayuan.search.utils.StringUtils.isBlank;
 * import static com.huayuan.search.utils.StringUtils.isNotBlank;
 */

/**
 * Created by wangchuanfu on 20/7/10.
 */
public class WebProductIndexServiceImpl {

 /**
    @Service
    public class WebAbroadProductIndexServiceImpl extends AbstractEsService <WebAbroadProductVo> implements WebAbroadProductIndexService {

        private static Logger logger = LoggerFactory.getLogger ( WebAbroadProductIndexServiceImpl.class );

        @Autowired
        Client esClientBean;

        @Autowired
        private CommonDBService dbService;

        public WebAbroadProductIndexServiceImpl() {
        }

        //索引id所使用的字段

        private final String ID_FIELD = ProductFieldEnum.productId.name ();
        //用于单机操作索引时注入ES客户端j1


        public WebAbroadProductIndexServiceImpl(Client clientBean) {
            this.esClientBean = clientBean;
        }

        @Override
        protected Client esClient() {
            return esClientBean;
        }

        // ======================================索引方法=======================================

        // 根据ID判断索引是否存在


        public boolean indexExists(String indexName, String typeName, Long productId) throws Exception {
            GetResponse response = esClient ().prepareGet ( indexName, typeName, productId.toString () ).execute ().actionGet ();
            return response.isExists ();
        }

        //创建空索引库，先判断索引是否存在，不存在才进行创建

        public boolean createIndex(String indexName) {
            try {
                if (indexExists ( indexName )) {
                    return true;
                } else {
                    return this.esClient ().admin ().indices ().prepareCreate ( indexName ).execute ().actionGet ().isAcknowledged ();
                }
            } catch (Exception e) {
                logger.error ( "[搜索服务]:索引创建失败...", e );
                e.printStackTrace ();
            }
            return false;
        }

        //删除索引库，先判断索引是否存在，存在才进行删除

        public boolean deleteIndex(String indexName) {
            try {
                if (indexExists ( indexName )) {
                    return this.esClient ().admin ().indices ().prepareDelete ( indexName ).execute ().actionGet ().isAcknowledged ();
                } else {
                    return true;
                }
            } catch (Exception e) {
                logger.error ( "[搜索服务]：索引删除失败...", e );
                e.printStackTrace ();
            }

            return false;
        }

        //不检查索引是否存在，直接删除多个索性

        public DeleteIndexResponse deleteIndex(String... indexNames) {
            try {
                return this.esClient ().admin ().indices ().prepareDelete ( indexNames ).execute ().actionGet ();
            } catch (Exception e) {
                e.printStackTrace ();
            }
            return null;

        }

        // 初始化所有索引，取产品Id作为索引Id

        public int initAllIndex(String idField, String indexName, String typeName, List <Map <String, Object>> list) throws Exception {
            int count = 0;
            // 创建批量请求
            BulkRequestBuilder bulkRequest = esClient ().prepareBulk ();
            // 获取所有产品列表
            for (Map <String, Object> product : list) {
                String produceId = product.get ( idField ).toString ();
                if (produceId != null && !"".equals ( produceId )) {
                    // 循环产品列表，逐个生成Source，加入批量请求
                    bulkRequest.add ( esClient ().prepareIndex ( indexName, typeName, produceId ).setSource ( getDoc ( product ) ) );
                    count++;
                }
            }
            if (logger.isDebugEnabled ())
                logger.debug ( JSON.toJSONString ( bulkRequest.request ().requests () ) );
            // 发送批量请求，获取批量回调
            BulkResponse bulkResponse = bulkRequest.execute ().actionGet ();
            if (bulkResponse.hasFailures ()) {
                // 如果批量回调返回失败，写日志，打印错误
                // TODO写日志
                System.err.println ( bulkResponse.buildFailureMessage () );
                count = -1;
            }
            return count;
        }


  *创建单条索引，取产品Id作为索引Id
  *


        public String addIndex(String indexName, String typeName, Long productId, Map <String, Object> objMap) throws Exception {
            String rid = null;
            if (objMap != null && objMap.size () != 0 && !indexExists ( indexName, typeName, productId )) {
                IndexRequestBuilder request = esClient ().prepareIndex ( indexName, typeName, productId.toString () ).setSource ( getDoc ( objMap ) );
// 添加索引
                IndexResponse response = request.execute ().actionGet ();
                rid = response.getId ();
            } else {
                rid = this.updateIndex ( indexName, typeName, productId, objMap );
            }
            return rid;
        }



 *更新单条索引


        public String updateIndex(String indexName, String typeName, Long productId, Map <String, Object> objMap) throws Exception {
            String rid = null;
            if (objMap != null && objMap.size () != 0 && indexExists ( indexName, typeName, productId )) {
                UpdateRequestBuilder request = esClient ().prepareUpdate ( indexName, typeName, productId.toString () ).setDoc ( getDoc ( objMap ) );
                UpdateResponse response = request.execute ().actionGet ();
                rid = response.getId ();
            }
            return rid;
        }


 *如果存在就更新，不存在就新增

 *@throws Exception

        public String addOrUpdateIndex(String indexName, String typeName, Long productId, Map <String, Object> objMap) throws Exception {
            String rid = null;
            boolean flag = indexExists ( indexName, typeName, productId );
            if (!flag && objMap != null && objMap.size () != 0) {
                IndexResponse response = esClient ().prepareIndex ( indexName, typeName, productId.toString () ).setSource ( getDoc ( objMap ) ).execute ().actionGet ();
                rid = response.getId ();
            } else if (objMap != null && objMap.size () != 0 && flag) {
                UpdateResponse response = esClient ().prepareUpdate ( indexName, typeName, productId.toString () ).setDoc ( getDoc ( objMap ) ).execute ().actionGet ();
                rid = response.getId ();
            } else if ((objMap == null || objMap.size () == 0) && flag) {
                DeleteResponse response = esClient ().prepareDelete ( indexName, typeName, productId.toString () ).execute ().actionGet ();
                rid = response.getId ();
            }
            return rid;
        }


 *设置索引的Mapping，Mapping用于指定索引字段的个性化操作方式。不创建Mapping时，ES会使用默认设置对索引字段进行设置。
                *注意：Mapping仅可设置一次，重新设置需要重加索引


        public PutMappingResponse putMapping(String indexName, String typeName, XContentBuilder mapping) {
            try {
                PutMappingRequest mappingRequest = Requests.putMappingRequest ( indexName ).type ( typeName ).source ( mapping );
                return this.esClient ().admin ().indices ().putMapping ( mappingRequest ).actionGet ();
            } catch (Exception e) {
                e.printStackTrace ();
            }
            return null;
        }

 *如果该域名不存在,则加入

        @Override

        public void addMappingRequest(String indexName, String typeName, String mapping) {
            try {
                PutMappingRequest mappingRequest = Requests.putMappingRequest ( indexName ).type ( typeName ).source ( mapping );
                this.esClient ().admin ().indices ().putMapping ( mappingRequest ).actionGet ();
            } catch (Exception e) {
                e.printStackTrace ();
            }
        }

  *根据实体对象上的Annotation配置创建Mapping
  *
          *@return
          *@throws Exception

        public XContentBuilder createMapping() throws Exception {
// 不要Format操作改变mapping的显示样式
            XContentBuilder mapping = XContentFactory.jsonBuilder ().startObject ().startObject ( indexTypes[0] )// 索引类型
                    .startObject ( "properties" );// 索引下的字段（固定标签）
// 存储，并且索引的String类型字段
            for (int i = 0; i < fields.length; i++) {
                IndexField field = fields[i].getAnnotation ( IndexField.class );
                String tindex_name = field.index_name ();
                if (isBlank ( tindex_name )) {
                    tindex_name = fields[i].getName ();
                }
                mapping.startObject ( tindex_name );// 字段名
                String tstore = field.store ();
                if (isNotBlank ( tstore ))
                    mapping.field ( "store", tstore );
                String tindex = field.index ();
                if (isNotBlank ( tindex ))
                    mapping.field ( "index", tindex );
                String ttype = field.type ();
                if (isNotBlank ( ttype ))
                    mapping.field ( "type", ttype );
                String tindex_analyzer = field.index_analyzer ();
                if (isNotBlank ( tindex_analyzer ))
                    mapping.field ( "index_analyzer", tindex_analyzer );
                String tsearch_analyzer = field.search_analyzer ();
                if (isNotBlank ( tsearch_analyzer ))
                    mapping.field ( "search_analyzer", tsearch_analyzer );
                mapping.endObject ();
            }
// 索引标识符，更新日期
            mapping.startObject ( DefaultIndexField.MODIFIED )
// 字段名
                    .field ( "type", "date" ).field ( "index", "analyzed" ).field ( "store", "yes" ).endObject ();
            mapping.endObject ().endObject ().endObject ();
            return mapping;
        }


 *创建Mapping对象。Mapping对象的Json格式示例：

        {
            "productIndex":{
            "properties":{
 *"title":{
                    "type":"string", "store":"yes"
                },"description":{
 *"type":"string", "index":"not_analyzed"
                },"price":{
                    "type":"double"
                },
 *"onSale":{
                    "type":"boolean"
                },"type":{
                    "type":"integer"
                },
 *"createDate":{
                    "type":"date"
                }
            }
        }
        }
 *注意：虽然字段的索引用分词器与搜索用分词器能进行区别设置，但是碍于不同分词器之间实现算法与词库的不同 ，ES官方建议尽量使用统一设置。
                *
                *@return
                *@throws Exception

// public XContentBuilder createMapping(String typeName, String[]
// analyzeFields, String[] sortDoubleFields, String[] storeFields
// , String[] storeDoubleFields) throws Exception
// {
// //不要Format操作改变mapping的显示样式
// XContentBuilder mapping = XContentFactory.jsonBuilder()
// .startObject()
// .startObject(typeName)//索引类型
// // .field("index_analyzer","ik")//为类型指定全局分词器
// .startObject("properties");//索引下的字段（固定标签）
// //存储，并且索引的String类型字段
// for(int i = 0; i < analyzeFields.length; i++)
// {
// if(analyzeFields[i] != null && !"".equals(analyzeFields[i].trim()))
// mapping.startObject(analyzeFields[i])//字段名
// .field("type", "string")//字段类型
// .field("term_vector", "with_positions_offsets")//可能的偏移值
// .field("index", "analyzed")//no不索引，analyzed分词后索引，not_analyzed不分词索引
// .field("index_analyzer", EsParams.INDEX_ANALYZER)//索引用分词器
// .field("search_analyzer", EsParams.SEARCH_ANALYZER)//搜索用分词器
// .field("store", "yes")//是否存储
// .endObject();
// }
// //存储，并且索引用以排序的Double类型字段
// for(int i = 0; i < sortDoubleFields.length; i++)
// {
// if(sortDoubleFields[i] != null && !"".equals(sortDoubleFields[i].trim()))
// mapping.startObject(sortDoubleFields[i])//字段名
// .field("type", "double")//字段类型
// .field("index", "not_analyzed")//no不索引，analyzed分词后索引，not_analyzed不分词索引
// .field("index_analyzer", EsParams.INDEX_ANALYZER)//索引用分词器
// .field("search_analyzer", EsParams.SEARCH_ANALYZER)//搜索用分词器
// .field("store", "yes")//是否存储
// .endObject();
// }
// //仅存储，不索引的String类型字段
// for(int i = 0; i < storeFields.length; i++)
// {
// if(storeFields[i] != null && !"".equals(storeFields[i].trim()))
// mapping.startObject(storeFields[i])//字段名
// .field("type", "string")
// .field("index", "no")
// .field("store", "yes")
// .endObject();
// }
// //仅存储，不索引的Double类型字段
// for(int i = 0; i < storeDoubleFields.length; i++)
// {
// if(storeDoubleFields[i] != null &&
// !"".equals(storeDoubleFields[i].trim()))
// mapping.startObject(storeDoubleFields[i])//字段名
// .field("type", "double")
// .field("index", "no")
// .field("store", "yes")
// .endObject();
// }
// //索引标识符，更新日期
// mapping.startObject(DefaultIndexField.MODIFIED)//字段名
// .field("type", "date")
// .field("index", "analyzed")
// .field("store", "yes")
// .endObject();
// mapping.endObject()
// .endObject()
// .endObject();
// return mapping;
// }


 *根据DefaultIndexField创建默认Mapping


// public XContentBuilder createDefaultMapping(String indexName) throws
// Exception
// {
// return this.createMapping(indexName, DefaultIndexField.analyzeFields,
// DefaultIndexField.sortDoubleFields
// , DefaultIndexField.storeFields, DefaultIndexField.storeDoubleFields);
// }



        public XContentBuilder createFeildMapping(String fieldName, String typeName) {
// 不要Format操作改变mapping的显示样式
            XContentBuilder mapping = null;
            try {
                mapping = XContentFactory.jsonBuilder ().startObject ().startObject ( indexTypes[0] )// 索引类型
                        .startObject ( "properties" );
                mapping.startObject ( fieldName );// 字段名
                mapping.field ( "type", typeName );
                mapping.endObject ();
                mapping.endObject ().endObject ().endObject ();
            } catch (IOException e) {
// TODO Auto-generated catch block
                e.printStackTrace ();
            }// 索引下的字段（固定标签）
            return mapping;
        }


 *获得index,type的Mapping设置

        public MappingMetaData getMapping(String indexName, String typeName) {
            try {
                ClusterState cs = this.esClient ().admin ().cluster ().prepareState ()
// index的名称
                        .setIndices ( indexName ).execute ().actionGet ().getState ();
                IndexMetaData indexMetaDate = cs.getMetaData ()
// index的名称
                        .index ( indexName );
// type的名称
                return indexMetaDate.mapping ( typeName );
            } catch (Exception e) {
                e.printStackTrace ();
            }

            return null;
        }


 *构建内容对象


        private XContentBuilder getDoc(Map <String, Object> product, String allPromotes, Map <Long, String> otherPromotes) {
            XContentBuilder sources = null;
            try {
                sources = XContentFactory.jsonBuilder ().startObject ();
                for (int i = 0; i < fields.length; i++) {
                    String fieldName = fields[i].getName ();
// 如果当前字段等于高品促销ids
                    if (fieldName.equals ( NewProductFieldEnum.promoteIds.name () )) {

                        if (product.get ( NewProductFieldEnum.goodsId.name () ) != null && product.get ( NewProductFieldEnum.goodsId.name () ) != "") {
                            String goodidstr = product.get ( NewProductFieldEnum.goodsId.name () ).toString ();
                            Long goodsId = Long.valueOf ( goodidstr );

                            String otherPromote = otherPromotes.get ( goodsId );
                            if (allPromotes != null && otherPromote != null) {
                                sources.field ( NewProductFieldEnum.promoteIds.name (), allPromotes + "," + otherPromote );
                            } else if (allPromotes != null && otherPromote == null) {
                                sources.field ( NewProductFieldEnum.promoteIds.name (), allPromotes );
                            } else if (allPromotes == null && otherPromote != null) {
                                sources.field ( NewProductFieldEnum.promoteIds.name (), otherPromote );
                            }

                        }

                        continue;
                    }
                    if (product.containsKey ( fieldName )) {

                        Object tFieldValue = product.get ( fieldName );

                        if (tFieldValue != null && !"".equals ( tFieldValue.toString ().trim () )) {
// 价格为用于排序的字段，如果价格为空，就给一个默认值-1
                            if (fieldName.equals ( ProductFieldEnum.ecPrice.name () ) || fieldName.equals ( ProductFieldEnum.marketPrice.name () )) {
                                if (tFieldValue == null || "".equals ( tFieldValue.toString ().trim () )) {

                                    sources.field ( fieldName, DefaultParams.INTEGER_EMPTY_VALUE );
                                } else {
// 价格去首位空格
                                    if (tFieldValue.toString ().startsWith ( "." )) {

                                        sources.field ( fieldName, tFieldValue != null ? "0" + tFieldValue.toString ().trim () : "0" );
                                    } else {

                                        sources.field ( fieldName, tFieldValue != null ? tFieldValue.toString ().trim () : "0" );
                                    }
                                }
                            }
// 总销量为用于排序的字段，如果总销量为空，就给一个默认值0
                            else if (fieldName.equals ( ProductFieldEnum.saleAmount.name () ) && (tFieldValue == null || "".equals ( tFieldValue.toString ().trim () ))) {

                                sources.field ( fieldName, 0 );
                            } else if (fieldName.equals ( ProductFieldEnum.attrs.name () )) {
// 产品属性集
                                List <Map <String, Object>> attrList = (List <Map <String, Object>>) product.get ( fieldName );
                                StringBuffer sb = new StringBuffer ();
                                for (Map <String, Object> attr : attrList) {
                                    try {
                                        if (attr.containsKey ( "attrId" ) && attr.containsKey ( "attrCode" )) {
                                            sb.append ( attr.get ( "attrId" ).toString () + "_" + attr.get ( "attrCode" ).toString () + "," );
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace ();
                                    }
                                }
// 附加上品牌
                                if (product.containsKey ( "productBrandId" ) && product.containsKey ( "productBrandName" ) && product.get ( "productBrandId" ) != null
                                        && product.get ( "productBrandName" ) != null) {
                                    try {
                                        String tproductBrandId = product.get ( "productBrandId" ).toString ();
                                        String tproductBrandName = product.get ( "productBrandName" ).toString ();
                                        if (!"".equals ( tproductBrandId.trim () ) && !"".equals ( tproductBrandName.trim () )) {
                                            sb.append ( "0_" + tproductBrandId + "," );
                                        }
                                    } catch (Exception e) {
                                        logger.error ( "[搜索服务]：构造内容对象失败...", e );
                                    }

                                }

                                if (sb.length () > 0) {
                                    sb.delete ( sb.length () - 1, sb.length () );
                                }
                                sources.field ( fieldName, sb.toString () );
                            } else {
// 如果商品卖家为空，默认为健一网商品
                                if (fieldName.equals ( "goodsSeller" )) {
                                    sources.field ( fieldName, tFieldValue != null ? tFieldValue.toString () : "1" );
                                } else {
                                    sources.field ( fieldName, tFieldValue != null ? tFieldValue.toString () : "" );
                                }

                            }
                        }
                    }
                }
// 关键域的设置
                sources.field ( DefaultIndexField.MODIFIED, new Date () );

// 销量得分
                if (product.containsKey ( ProductFieldEnum.saleScore.toString () )) {
// 销量权重*10
                    sources.field (
                            ProductFieldEnum.saleScore.toString (),
                            product.get ( ProductFieldEnum.saleScore.toString () ) != null ? Double.parseDouble ( product.get ( ProductFieldEnum.saleScore.toString () )
                                    .toString () ) * 10 : 0 );
                } else {
                    sources.field ( ProductFieldEnum.saleScore.toString (), 0 );
                }

// 关键词权重
                if (product.containsKey ( DefaultIndexField.KEYWORD ) && product.get ( DefaultIndexField.KEYWORD ) != null) {
                    List <Map <String, Object>> keywordList = (List <Map <String, Object>>) product.get ( DefaultIndexField.KEYWORD );
                    for (Map <String, Object> keywordMap : keywordList) {
                        String keywordName = keywordMap.get ( "keyword" ).toString ();
                        Double keywordScore = new Double ( 0 );
// 如果keywordscore转换失败
                        try {
// 人工维护权重 1-1000
                            keywordScore = Double.parseDouble ( keywordMap.get ( "score" ).toString () );
                        } catch (Exception e) {
                            logger.error ( "[搜索服务]：keywordScore parse error", e );
                            keywordScore = 0d;
                        }

                        if (StringUtils.isNotBlank ( keywordName )) {
                            sources.field ( keywordName, keywordScore );
                        }
                    }
                }
                sources.endObject ();
            } catch (Exception e) {
                logger.error ( "[搜索服务]：构造内容对象失败...", e );
                e.printStackTrace ();
            }
            return sources;
        }


 *生成source
 *
         *
        @param
        product
 *
        product Map对象
 *@return doc对象

        @SuppressWarnings("unchecked")

        public XContentBuilder getDoc(Map <String, Object> product) {
            XContentBuilder sources = null;
            try {
                sources = XContentFactory.jsonBuilder ().startObject ();
                for (int i = 0; i < fields.length; i++) {
                    String fieldName = fields[i].getName ();
                    if (fieldName.equals ( NewProductFieldEnum.promoteIds.name () )) {
                        continue;
                    }
                    if (product.containsKey ( fieldName )) {
                        Object tFieldValue = product.get ( fieldName );
                        if (tFieldValue != null && !"".equals ( tFieldValue.toString ().trim () )) {
                            // 价格为用于排序的字段，如果价格为空，就给一个默认值-1
                            if (fieldName.equals ( ProductFieldEnum.ecPrice.name () ) || fieldName.equals ( ProductFieldEnum.marketPrice.name () )) {
                                if (tFieldValue == null || "".equals ( tFieldValue.toString ().trim () ))
                                    sources.field ( fieldName, DefaultParams.INTEGER_EMPTY_VALUE );
                                else {
                                    // 价格去首位空格
                                    if (tFieldValue.toString ().startsWith ( "." ))
                                        sources.field ( fieldName, tFieldValue != null ? "0" + tFieldValue.toString ().trim () : "0" );
                                    else
                                        sources.field ( fieldName, tFieldValue != null ? tFieldValue.toString ().trim () : "0" );
                                }
                            }
                            // 总销量为用于排序的字段，如果总销量为空，就给一个默认值0
                            else if (fieldName.equals ( ProductFieldEnum.saleAmount.name () ) && (tFieldValue == null || "".equals ( tFieldValue.toString ().trim () )))
                                sources.field ( fieldName, 0 );
                            else if (fieldName.equals ( ProductFieldEnum.attrs.name () )) {
                                // 产品属性集
                                List <Map <String, Object>> attrList = (List <Map <String, Object>>) product.get ( fieldName );
                                StringBuffer sb = new StringBuffer ();
                                for (Map <String, Object> attr : attrList) {
                                    try {
                                        if (attr.containsKey ( "attrId" ) && attr.containsKey ( "attrCode" )) {
                                            sb.append ( attr.get ( "attrId" ).toString () + "_" + attr.get ( "attrCode" ).toString () + "," );
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace ();
                                    }
                                }
                                // 附加上品牌
                                if (product.containsKey ( "productBrandId" ) && product.containsKey ( "productBrandName" ) && product.get ( "productBrandId" ) != null
                                        && product.get ( "productBrandName" ) != null) {
                                    try {
                                        String tproductBrandId = product.get ( "productBrandId" ).toString ();
                                        String tproductBrandName = product.get ( "productBrandName" ).toString ();
                                        if (!"".equals ( tproductBrandId.trim () ) && !"".equals ( tproductBrandName.trim () )) {
                                            sb.append ( "0_" + tproductBrandId + "," );
                                        }
                                    } catch (Exception e) {
                                        logger.error ( e.getMessage (), e );
                                    }

                                }
                                if (sb.length () > 0)
                                    sb.delete ( sb.length () - 1, sb.length () );
                                sources.field ( fieldName, sb.toString () );
                            }
                            // else
                            // if(fieldName.equals(ProductFieldEnum.cataLogListId.name())){
                            // // 产品属性集
                            // String catalogListIds=product.get(fieldName)!=null ?
                            // product.get(fieldName).toString() :"";
                            // String value="";
                            // if(!"".equals(catalogListIds)){
                            // String [] arrStr =catalogListIds.split(",");
                            // List<String> listStr=Arrays.asList(arrStr);
                            // Set<String> setStr=new HashSet<String>(listStr);
                            // value=StringUtils.join(setStr, ",");
                            // }
                            // sources.field(fieldName, value);
                            // }
                            else
                                sources.field ( fieldName, tFieldValue != null ? tFieldValue.toString () : "" );
                        }
                    }
                }
                // 关键域的设置
                sources.field ( DefaultIndexField.MODIFIED, new Date () );

                // 销量得分
                if (product.containsKey ( ProductFieldEnum.saleScore.toString () )) {
                    // 销量权重*10
                    sources.field (
                            ProductFieldEnum.saleScore.toString (),
                            product.get ( ProductFieldEnum.saleScore.toString () ) != null ? Double.parseDouble ( product.get ( ProductFieldEnum.saleScore.toString () )
                                    .toString () ) * 10 : 0 );
                } else {
                    sources.field ( ProductFieldEnum.saleScore.toString (), 0 );
                }

                // 关键词权重
                if (product.containsKey ( DefaultIndexField.KEYWORD ) && product.get ( DefaultIndexField.KEYWORD ) != null) {
                    List <Map <String, Object>> keywordList = (List <Map <String, Object>>) product.get ( DefaultIndexField.KEYWORD );
                    for (Map <String, Object> keywordMap : keywordList) {
                        String keywordName = keywordMap.get ( "keyword" ).toString ();
                        Double keywordScore = new Double ( 0 );
                        // 如果keywordscore转换失败
                        try {
                            // 人工维护权重 1-1000
                            keywordScore = Double.parseDouble ( keywordMap.get ( "score" ).toString () );
                        } catch (Exception e) {
                            logger.error ( "keywordScore parse error", e );
                            keywordScore = 0d;
                        }

                        if (StringUtils.isNotBlank ( keywordName )) {
                            sources.field ( keywordName, keywordScore );
                        }
                    }
                }
                sources.endObject ();
            } catch (Exception e) {
                e.printStackTrace ();
            }
            return sources;
        }

        // ====================接口用的索引方法
        public int createIndexAndPutMapping(String indexName, String typeName) throws Exception {
            int count = 0;
            long start = System.currentTimeMillis ();
            // 1.获得所有商品
            logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 全量检索数据开始..." );
            long start3 = System.currentTimeMillis ();
            List <Map <String, Object>> list = new ArrayList <Map <String, Object>> ();
            list = dbService.getAbroadProductList ();
            long end3 = System.currentTimeMillis ();
            logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 全量检索数据结束...耗时" + (end3 - start3) + "毫秒" );
            // 2.遍历list，重新构建索引
            if (null != list && list.size () > 0) {
                // 1.生成索引所需数据
                long start1 = System.currentTimeMillis ();
                logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 生成检索数据开始..." );
                Object[] obj = createIndexRequest ( ID_FIELD, indexName, typeName, list );
                if (null != obj && obj.length == 2 && null != obj[0] && null != obj[1]) {
                    BulkRequestBuilder bulkRequest = (BulkRequestBuilder) obj[0];
                    count = (Integer) obj[1];
                    long end1 = System.currentTimeMillis ();
                    logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 生成检索数据结束...耗时" + (end1 - start1) + "毫秒" );

                    // 2.删除原索引，新建索引
                    long start2 = System.currentTimeMillis ();
                    logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 刪除原有索引，新建索引开始..." );
                    if (indexExists ( getIndexName () )) {
                        deleteIndex ( getIndexName () );
                    }
                    // 创建索引
                    createIndex ( indexName );
                    // 设置索引mapping
                    putMapping ( indexName, typeName, createMapping () );

                    long end2 = System.currentTimeMillis ();
                    logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 刪除原有索引，新建索引结束...耗时" + (end2 - start2) + "毫秒" );

                    // 3.创建索引数据
                    long start4 = System.currentTimeMillis ();
                    logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 创建索引数据开始..." );

                    int tcount = initAllIndex ( indexName, typeName, bulkRequest );
                    count = (tcount == -1) ? tcount : count;
                    long end4 = System.currentTimeMillis ();
                    logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + " 创建索引数据结束...耗时" + (end4 - start4) + "毫秒" );

                    long end = System.currentTimeMillis ();
                    String msg = " 总商品数：" + list.size () + "，，初始化索引数：" + count + "，总时间：" + (end - start) + " 毫秒";
                    logger.info ( "[搜索服务]:" + "时间：" + DateUtils.formatDate ( new Date () ) + msg );
                }
            }

            return count;
        }

  *初始化所有索引，取产品Id作为索引Id


        public int initAllIndex(String indexName, String typeName, BulkRequestBuilder bulkRequest) throws Exception {
            int count = 0;
// 发送批量请求，获取批量回调
            BulkResponse bulkResponse = bulkRequest.execute ().actionGet ();
            if (bulkResponse.hasFailures ()) {
// 如果批量回调返回失败，写日志，打印错误
                logger.error ( "[搜索服务]:初始化所有索引失败..." + bulkResponse.buildFailureMessage () );
                count = -1;
            }
            return count;
        }

        @Autowired
        private PromoteProductService promoteProductService;

        private Object[] createIndexRequest(String idField, String indexName, String typeName, List <Map <String, Object>> list) throws Exception {
            int count = 0;
            // 创建批量请求
            BulkRequestBuilder bulkRequest = esClient ().prepareBulk ();
            // 查询商品参加的促销列表
            String allPromotes = promoteProductService.queryAllPromotes ();
            Map <Long, String> otherPromotes = promoteProductService.queryOtherPromotes ();
            Client client = esClient ();
            // 获取所有产品列表
            for (Map <String, Object> product : list) {
                String produceId = product.get ( idField ).toString ();
                if (produceId != null && !"".equals ( produceId )) {
                    XContentBuilder source = getDoc ( product, allPromotes, otherPromotes );
                    // 循环产品列表，逐个生成Source，加入批量请求
                    bulkRequest.add ( client.prepareIndex ( indexName, typeName, produceId ).setSource ( source ) );
                    count++;
                }
            }
            Object[] obj = new Object[]{bulkRequest, count};
            return obj;
        }


        public String addOrUpdateIndex(String indexName, String typeName, Long productId) throws Exception {
            String result = null;
            Map <String, Object> objMap = dbService.getProductQueryById ( productId );
            boolean flag = indexExists ( indexName, typeName, productId );
            if (!flag && objMap != null && objMap.size () != 0) {
                IndexResponse response = esClient ().prepareIndex ( indexName, typeName, productId.toString () ).setSource ( getDoc ( objMap ) ).execute ().actionGet ();
                result = response.getId ();
            } else if (objMap != null && objMap.size () != 0 && flag) {
                UpdateResponse response = esClient ().prepareUpdate ( indexName, typeName, productId.toString () ).setDoc ( getDoc ( objMap ) ).execute ().actionGet ();
                result = response.getId ();
            } else if ((objMap == null || objMap.size () == 0) && flag) {
                DeleteResponse response = esClient ().prepareDelete ( indexName, typeName, productId.toString () ).execute ().actionGet ();
                result = "" + !response.isFound ();
            }
            return result;
        }


  *批量删除

        public void deleteBlukIndex(String indexName, String typeName, List <Long> productId) throws Exception {
            if (productId != null && productId.size () != 0) {
                BulkRequestBuilder bulkRequest = esClient ().prepareBulk ();
                for (Long tproductId : productId) {
                    bulkRequest.add ( esClient ().prepareDelete ( indexName, typeName, tproductId.toString () ) );
                }
                BulkResponse bulkResponse = bulkRequest.execute ().actionGet ();
                if (bulkResponse.hasFailures ()) {
                    // TODO 错误日志
                }
            }
        }

        @Override
        public String bulkUpdateIndex(String indexName, String typeName, List <Map <String, Object>> productsScoreList) {
            String rid = null;
            BulkRequestBuilder bulkRequest = esClient ().prepareBulk ();
            for (Map <String, Object> productScore : productsScoreList) {
                bulkRequest.add ( esClient ().prepareUpdate ( indexName, typeName, productScore.get ( "id" ).toString () ).setDoc ( getKeywordScoreDoc ( productScore ) ) );
            }

            BulkResponse bulkResponse = bulkRequest.execute ().actionGet ();
            if (bulkResponse.hasFailures ()) {
                // TODO 错误日志
            }
            return null;
        }


  *
        更新关键字Score索引 生成source

        public XContentBuilder getKeywordScoreDoc(Map <String, Object> keywordMap) {
            XContentBuilder sources = null;
            try {
                sources = XContentFactory.jsonBuilder ().startObject ();
                String keyword = keywordMap.get ( "keyword" ).toString ();
                String score = keywordMap.get ( "totalScore" ).toString ();

                if (StringUtils.isNotBlank ( keyword )) {
                    sources.field ( keyword, score );
                }
                sources.endObject ();
            } catch (Exception e) {
                e.printStackTrace ();
            }
            return sources;
        }

    }
  */
}
