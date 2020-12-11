package com.lisi;


import com.fasterxml.jackson.core.JsonFactory;
import io.netty.handler.codec.json.JsonObjectDecoder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

public class EsTest {
    private PreBuiltTransportClient client;

    @SuppressWarnings("unchecked")
    @Before
    public void getClient() throws UnknownHostException {
        Settings settings =Settings.builder().put("cluster.name", "my-application").build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop100"), 9300));
        System.out.println("clusterName="+client.nodeName());
    }

    /**
     * 创建索引
     */
    @Test
    public void createIndex_blog(){
        client.admin().indices().prepareCreate("blog").get();
        client.close();
    }

    /**
     * 通过json创建文档
     */
    @Test
    public void createDocumentByJson(){
        // 1 文档数据准备
        String json = "{" + "\"id\":\"2\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口2\"" + "}";
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2").setSource(json).execute().actionGet();

        System.out.println("index="+indexResponse.getIndex());
        System.out.println("type="+indexResponse.getType());
        System.out.println("id="+indexResponse.getId());
        System.out.println("version="+indexResponse.getVersion());
        System.out.println("result="+indexResponse.getResult());
        client.close();
    }

    /**
     * 通过map创建文档
     */
    @Test
    public void createIndexByMap(){
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("id","4");
        map.put("content","陈臣老铁写了本书，自传在天津的学校2");
        map.put("title","天津的学校");
        map.put("compony","远传");
        IndexResponse indexResponse = client.prepareIndex("blog2", "article2", "5").setSource(map).execute().actionGet();
        System.out.println("index="+indexResponse.getIndex());
        System.out.println("type="+indexResponse.getType());
        System.out.println("id="+indexResponse.getId());
        System.out.println("version="+indexResponse.getVersion());
        System.out.println("result="+indexResponse.getResult());
        client.close();
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteDocumet(){
        GetResponse response = client.prepareGet("blog", "article", "6").get();

        System.out.println("result="+response.getSourceAsString());
    }

    /**
     * 通过es自带的帮助类，构建json数据
     * @throws Exception
     */
    @Test
    public void createIndex() throws Exception {

        // 1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id", 5)
                .field("title", "基于Lucene的搜索服务器").field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口5。")
                .endObject();

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "6").setSource(builder).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());
        // 4 关闭连接
        client.close();
    }


    /**
     * 删除索引
     */
    @Test
    public void deleteIndex_blog(){
        client.admin().indices().prepareDelete("blog").get();
        client.close();
    }
}
