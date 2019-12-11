package org.bdcourse.sinks;
/*
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;


import org.elasticsearch.transport.client.PreBuiltTransportClient;


import java.net.InetAddress;

public class sink implements ElasticsearchSinkFunction<Tuple2<String, Double>> {


    private String indexName;
    private String typeName;

    public sink(String indexName, String typeName) {
        this.indexName = indexName;
        this.typeName = typeName;
    }


    @Override
    public void process(Tuple2<String, Double> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("country", t.f0)
                    .startObject("location")
                    .field("lat", t.f1.getLatitude())
                    .field("lon", t.f1.getLongitude())
                    .endObject()
                    .field("cnt", t.f2)
                    .endObject();
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress("0.0.0.0", 9300);

            IndexRequest indexRequest = client.prepareIndex(indexName, typeName).setSource(json).request();
            requestIndexer.add(indexRequest);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
*/