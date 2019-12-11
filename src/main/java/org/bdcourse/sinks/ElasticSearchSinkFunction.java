package org.bdcourse.sinks;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

public class ElasticSearchSinkFunction implements ElasticsearchSinkFunction<Tuple2<Integer, String>> {

    public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element.f1);

        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .id(element.f0.toString())
                .source(json);
    }

    public void process(Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }
}

