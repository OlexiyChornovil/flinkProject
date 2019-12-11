package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class SelectTweetHashtags implements FlatMapFunction<String, List<String>> {
    private transient ObjectMapper jsonParser;
    @Override
    public void flatMap(String value, Collector<List<String>> out) throws Exception {

        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
        if(hasHashtags) {
            JsonNode tmp = jsonNode.get("entities").get("hashtags");
            List<String> collector = new ArrayList<>();
            for (JsonNode jsonNode2 : tmp) {
                if(jsonNode2.has("text")) {
                    collector.add(jsonNode2.get("text").toString());
                }
            }
            out.collect(collector);
        }
    }
}
