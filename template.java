package com.pingan.elaticsearch.dto;

import java.util.Date;

public class TwtterDTO {

	private String user;

	private Date postDate;

	private String message;

	private String type;

	private String job;

	private String location;

	private String name;

	public String getJob() {
		return job;
	}

	public void setJob(String job) {
		this.job = job;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Date getPostDate() {
		return postDate;
	}

	public void setPostDate(Date postDate) {
		this.postDate = postDate;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}


package com.pingan.elaticsearch.dto;

import org.elasticsearch.common.xcontent.XContentBuilder;

public class UpdateHelperDTO {

	private String index;

	private String type;

	private String id;

	private XContentBuilder builder;

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public XContentBuilder getBuilder() {
		return builder;
	}

	public void setBuilder(XContentBuilder builder) {
		this.builder = builder;
	}

}

package com.pingan.elaticsearch.esenum;

public enum ElasticSearchEnum {

	ES_CLUSTER_NAME("ES_CLUSTER_NAME","clusterName","wali"),
	
	ES_CLUSTER_HOST("ES_CLUSTER_HOST","host","localhost"),
	
	ES_CLUSTER_PORT("ES_CLUSTER_PORT","port","9300"),

	;

	private ElasticSearchEnum(String key, String code, String value) {
		this.key = key;
		this.code = code;
		this.value = value;
	}

	private String key;

	private String code;

	private String value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}


package com.pingan.elaticsearch.service;

import java.util.Map;

public interface CountService {

	public long getMatchAllQueryCount(String index);

	public long getBoolQueryCount(String index, Map<String, Object> condition);

	public long getPhraseQueryCount(String index, String key, Object value);
}


package com.pingan.elaticsearch.service.impl;

import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.pingan.elaticsearch.service.CountService;
import com.pingan.elaticsearch.util.ESTransportClient;

public class CountServiceImpl implements CountService {

	private Client client = ESTransportClient.getClient();

	@Override
	public long getMatchAllQueryCount(String index) {
		QueryBuilder query = QueryBuilders.matchAllQuery();
		System.out.println("getMatchAllQueryCount query =>" + query.toString());
		return client.prepareSearch(index).setQuery(query).setSize(0).execute().actionGet().getHits().getTotalHits();
	}

	@Override
	public long getBoolQueryCount(String index, Map<String, Object> condition) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (Entry<String, Object> entry : condition.entrySet()) {
			query = query.must(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
		}
		System.out.println("getBoolQueryCount query =>" + query.toString());
		return client.prepareSearch(index).setQuery(query).setSize(0).execute().actionGet().getHits().getTotalHits();
	}

	@Override
	public long getPhraseQueryCount(String index, String key, Object value) {
		QueryBuilder query = QueryBuilders.matchPhraseQuery(key, value);
		System.out.println("getPhraseQueryCount query =>" + query.toString());
		return client.prepareSearch(index).setQuery(query).setSize(0).execute().actionGet().getHits().getTotalHits();
	}

}
package com.pingan.elaticsearch.service;

import java.util.List;
import java.util.Map;

public interface DataService {

	public void multiGet(String index, String type, String... keyWord);

	public List<String> getMatchAllQueryData(String index);

	public List<String> getBoolQueryData(String index, Map<String, Object> condition);

	public List<String> getPhraseQueryData(String index, String key, Object value);

}
package com.pingan.elaticsearch.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.pingan.elaticsearch.service.DataService;
import com.pingan.elaticsearch.util.ESTransportClient;

public class DataServiceImpl implements DataService {

	private Client client = ESTransportClient.getClient();

	@Override
	public void multiGet(String index, String type, String... ids) {
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add(index, type, ids).get();
		for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
			GetResponse response = itemResponse.getResponse();
			if (response.isExists()) {
				String jsonRes = response.getSourceAsString();
				System.out.println(jsonRes);
			}
		}
	}

	@Override
	public List<String> getMatchAllQueryData(String index) {
		QueryBuilder query = QueryBuilders.matchAllQuery();
		SearchHit[] hits = client.prepareSearch(index).setQuery(query).execute().actionGet().getHits().getHits();
		List<String> list = new ArrayList<String>();
		for (SearchHit hit : hits) {
			String jsonRes = hit.getSourceAsString();
			System.out.println(jsonRes);
			list.add(jsonRes);
		}
		return list;
	}

	@Override
	public List<String> getBoolQueryData(String index, Map<String, Object> condition) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (Entry<String, Object> entry : condition.entrySet()) {
			query = query.must(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
		}
		SearchHit[] hits = client.prepareSearch(index).setQuery(query).execute().actionGet().getHits().getHits();
		List<String> list = new ArrayList<String>();
		for (SearchHit hit : hits) {
			String jsonRes = hit.getSourceAsString();
			System.out.println(jsonRes);
			list.add(jsonRes);
		}
		return list;
	}

	@Override
	public List<String> getPhraseQueryData(String index, String key, Object value) {
		QueryBuilder query = QueryBuilders.matchPhraseQuery(key, value);
		SearchHit[] hits = client.prepareSearch(index).setQuery(query).execute().actionGet().getHits().getHits();
		List<String> list = new ArrayList<String>();
		for (SearchHit hit : hits) {
			String jsonRes = hit.getSourceAsString();
			System.out.println(jsonRes);
			list.add(jsonRes);
		}
		return list;
	}

}
package com.pingan.elaticsearch.service;

public interface DeleteService {

	public void delete(String index, String type, String id);

	public long deleteByMatchQuery(String index, String key, Object value);

	public long deleteByTermQuery(String index, String key, Object value);

}
package com.pingan.elaticsearch.service.impl;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

import com.pingan.elaticsearch.service.DeleteService;
import com.pingan.elaticsearch.util.ESTransportClient;

public class DeleteServiceImpl implements DeleteService {

	private Client client = ESTransportClient.getClient();
	
	@Override
	public void delete(String index, String type, String id) {
		client.prepareDelete(index, type, id);
	}

	@Override
	public long deleteByMatchQuery(String index, String key, Object value) {
		BulkByScrollResponse res = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery(key, value))
				// .filter(QueryBuilders.typeQuery(type))
				.source(index).get();;
		return res.getDeleted();
	}

	@Override
	public long deleteByTermQuery(String index, String key, Object value) {
		BulkByScrollResponse res = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.termQuery(key, value))
				// .filter(QueryBuilders.typeQuery(type))
				.source(index).get();
		return res.getDeleted();
	}

}
package com.pingan.elaticsearch.service;

import java.util.List;

public interface InsertService {

	public void ingest(String index, String type, String doc);

	public boolean ingest(String index, String type, List<String> docs);

}
package com.pingan.elaticsearch.service.impl;

import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;

import com.pingan.elaticsearch.service.InsertService;
import com.pingan.elaticsearch.util.ESTransportClient;

public class InsertServiceImpl implements InsertService {

	private Client client = ESTransportClient.getClient();

	@Override
	public void ingest(String index, String type, String doc) {
		client.prepareIndex(index, type).setSource(doc, XContentType.JSON).get();
	}

	@Override
	public boolean ingest(String index, String type, List<String> docs) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		docs.forEach(doc -> bulkRequest.add(client.prepareIndex(index, type).setSource(doc, XContentType.JSON)));
		return bulkRequest.get().hasFailures();
	}

}
package com.pingan.elaticsearch.service;

import java.util.List;

import com.pingan.elaticsearch.dto.UpdateHelperDTO;

public interface UpdateService {
	
	public void bulkUpdate(List<UpdateHelperDTO> updates);
	
}
package com.pingan.elaticsearch.service.impl;

import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import com.pingan.elaticsearch.dto.UpdateHelperDTO;
import com.pingan.elaticsearch.service.UpdateService;
import com.pingan.elaticsearch.util.ESTransportClient;

public class UpdateServiceImpl implements UpdateService {


	private Client client = ESTransportClient.getClient();

	@Override
	public void bulkUpdate(List<UpdateHelperDTO> updateHelperDTOs) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (UpdateHelperDTO updateHelperDTO : updateHelperDTOs) {
			String index = updateHelperDTO.getIndex();
			String type = updateHelperDTO.getType();
			String id = updateHelperDTO.getId();
			bulkRequest.add(client.prepareIndex(index, type, id).setSource(updateHelperDTO.getBuilder()));
		}
		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
			System.out.println("--- bulkUpdate error ---");
		}
	}

}
package com.pingan.elaticsearch.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.pingan.elaticsearch.esenum.ElasticSearchEnum;

/**
 * @author zhongjun001
 *
 */
public class ESTransportClient {

	public static TransportClient client = null;

	public static String clusterName = ElasticSearchEnum.ES_CLUSTER_NAME.getValue();

	public static String host = ElasticSearchEnum.ES_CLUSTER_HOST.getValue();

	public static Integer port = Integer.valueOf(ElasticSearchEnum.ES_CLUSTER_PORT.getValue());

	public static synchronized Client getClient() {
		try {
			if (null == client) {
				Settings settings = Settings.builder().put("cluster.name", clusterName).put("client.transport.sniff", true).build();
				client = new PreBuiltTransportClient(settings);
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.out.println("--- 获取elasticsearch客户端异常 ---");
		}
		return client;
	}

}
package com.pingan.elaticsearch.util;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;


public class EsUtils {
	
	private static Client client = ESTransportClient.getClient();
	
	public static void main(String[] args) {
		deleteAllIndex();
		//createIndex("jay2");
		//delIndex("jay2");
		//createMapping("jay","test3");
		//insertValue("jay","test2");
		//updateValue("jay","test2","AVxLJTcuPDAe1x9hgRqG");
		//queryById("jay","test2","AVxLO8D7PDAe1x9hgRqN");
		//deleteByQuery("jay","test2");
		//queryByFilter("jay","test2");
	}
	

	/**
	 * 判断索引是否存在
	 * 
	 * @param indexName
	 * @return
	 */
	private static boolean isIndexExist(String indexName){
	        IndicesExistsResponse inExistsResponse = client
	        		.admin()
	        		.indices()
	        		.exists(new IndicesExistsRequest(indexName))
	                .actionGet();
	        return inExistsResponse.isExists();
	}
	
	
	/**
	 * 创建索引
	 * 
	 * @param indexName
	 */
	public static void createIndex(String indexName){
		try{
			if(!isIndexExist(indexName)){
				CreateIndexResponse response = client.admin().indices().prepareCreate(indexName).get();
				//创建索引
				System.out.println("创建索引--->"+indexName+":"+response.isAcknowledged());
			}else{
				System.out.println("创建--->"+indexName+" 已经存在");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除索引
	 */
	public static void delIndex(String indexName){
		try{
			if(isIndexExist(indexName)){
				DeleteIndexResponse response = client.admin().indices().prepareDelete(indexName).get();
				System.out.println("删除索引 --->"+indexName+":"+response.isAcknowledged());
			}else{
				System.out.println("索引 --->"+indexName+"不存在");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	

	/**
	 * 删除所有索引
	 * 
	 */
	public static void deleteAllIndex() {
		ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet();
		// 查询所有索引
		String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
		for (String index : indexs) {
			// 删除索引
			DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
			if (deleteIndexResponse.isAcknowledged()) {
				System.out.println(index + " delete");
			}

		}
	}
	
	/**
	 * 3.创建映射
	 */
	public static void createMapping(String indexName,String type){
		try{
			XContentBuilder builder = XContentFactory.jsonBuilder()
											.startObject()
						                    .field("properties")
						                        .startObject()
						                            .field("title")
						                                .startObject()
						                                    .field("type", "string")
						                                .endObject()
						                            .field("age")
						                                .startObject()
						                                    .field("index", "not_analyzed")
						                                    .field("type", "integer")
						                                .endObject()
						                            .field("name")
						                                .startObject()
						                                    .field("type", "integer")
						                                .endObject()
						                             .endObject()
											.endObject();
			 System.out.println(builder.string());           
		     PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName).source(builder).type(type);
		     client.admin().indices().putMapping(mappingRequest).actionGet();
					
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 插入值
	 * 
	 * @param indexName
	 * @param type
	 */
	public static void insertValue(String indexName,String type){
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder()
			                .startObject()
			                    .field("name", "zhangsan")
			                    .field("age", 30)
			                .endObject();
			IndexResponse  indexResponse = client.prepareIndex()
                  .setIndex(indexName)
                  .setType(type)
                  .setSource(builder)
                  .get();
			System.out.println(indexResponse.status());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 更新值
	 * 
	 * @param indexName
	 * @param type
	 * @param id
	 */
	public static void updateValue(String indexName,String type,String id){
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder()
			                .startObject()
			                    .field("name", "jj")
			                .endObject();
			UpdateResponse updateResponse = 
		            	client
		                .prepareUpdate()
		                .setIndex(indexName)
		                .setType(type)
		                .setId(id)
		                .setDoc(builder)
		                .get();
			System.out.println(updateResponse.status());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 删除记录
	 */
	public static void delRecord(String indexName,String type,String id){
		try{
			DeleteResponse deleteResponse  = client
		            .prepareDelete()  
		            .setIndex(indexName)
		            .setType(type)
		            .setId(id)
		            .get();
			/*client.prepareDelete(indexName, type, id).get();*/
		    System.out.println(deleteResponse.status());
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 根据Id查询记录
	 * 
	 * @param indexName
	 * @param type
	 * @param id
	 */
	public static void queryById(String indexName,String type,String id){
		try{
			GetResponse getResponse = client
                    .prepareGet()   
                    .setIndex(indexName)  
                    .setType(type)
                    .setId(id)
                    .get();
			System.out.println(getResponse.toString());
			System.out.println("getSource:"+getResponse.getSourceAsMap().toString());
			System.out.println("getType:"+getResponse.getType());
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 过滤查询
	 * 
	 * @param indexName
	 * @param type
	 */
	public static void queryByFilter(String indexName, String type) {
		try{
			QueryBuilder queryBuilder = QueryBuilders.termQuery("name", "zhangsan");
			SearchResponse response = client.prepareSearch(indexName).setTypes(type)
			        .setQuery(queryBuilder)
			        .execute()
			        .actionGet();
			List<String> docList = new ArrayList<String>();
			SearchHits searchHits = response.getHits();
		    for (SearchHit hit : searchHits) {
		        docList.add(hit.getSourceAsString());
		    }
		    System.out.println(docList.toString());
		    System.out.println(response.getTookInMillis());
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
package com.pingan.elaticsearch.util;

import java.util.List;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by T T on 2017/6/27.
 */
public class SearchTemplate{
   
	private Client client = ESTransportClient.getClient();
	

    /**
     * 批量创建文档，需指定索引和类型
     * @param index 索引
     * @param type  类型
     * @param docs  文档
     * @return
     */
    public BulkResponse createIndex(String index, String type, List<String> docs){
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for(String doc : docs){
            bulkRequestBuilder.add(client.prepareIndex(index,type).setSource(doc));
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        return bulkResponse;
    }

    /**
     * 单个创建文档，需指定索引和类型
     * @param index 索引
     * @param type  类型
     * @param doc   文档
     * @return
     */
    public IndexResponse createIndex(String index, String type, String doc){
        IndexResponse indexResponse = client.prepareIndex(index, type).setSource(doc).get();
        return indexResponse;
    }

    /**
     * 指定索引、类型和搜索类型进行搜索，若类型为空默认对整个索引进行搜索
     * @param index 索引
     * @param type  类型
     * @param queryBuilder  搜索类型
     * @return
     */
    public SearchResponse search(String index, String type, QueryBuilder queryBuilder){
        if (type == null|| type.length() == 0){
            return client.prepareSearch(index).setQuery(queryBuilder).get();
        }
        return client.prepareSearch(index).setTypes(type).setQuery(queryBuilder).get();
    }

    /**
     * 根据id删除指定索引、类型下的文档，id需先通过搜索获取
     * @param index 索引
     * @param type  类型
     * @param id    文档id
     * @return
     */
    public DeleteResponse deleteIndex(String index,String type,String id) {
        DeleteResponse deleteResponse= client.prepareDelete(index, type, id).get();
        return deleteResponse;
    }
    /**
     * 删除指定索引，慎用
     * @param index 索引
     * @return
     */
    public DeleteIndexResponse deleteAll(String index){
        DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).get();
        return deleteIndexResponse;
    }

    /**
     * 根据index、type、id更新文档
     * @param index 索引
     * @param type  文档
     * @param id    文档id
     * @param newDoc 更新后的文档json字符串
     * @return
     */
    public Boolean update(String index,String type,String id,String newDoc){
        UpdateResponse updateResponse = client.prepareUpdate(index,type,id).setDoc(newDoc).get();
        if(updateResponse.getResult()!= DocWriteResponse.Result.UPDATED){
            return false;
        }
        return true;
    }

    /**
     * 判断指定Index是否存在
     * @param index
     * @return
     */
    public Boolean indexExist(String index){
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }
}


