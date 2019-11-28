package org.bdcourse.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class TwitterJsonToTuple implements FlatMapFunction<String, Tuple2<String, String>>{

    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {

        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
        if(hasHashtags) {
            String a = jsonNode.get("entities").get("hashtags").toString();
            JsonNode b = jsonNode.get("entities").get("hashtags");
            for (JsonNode jsonNode2 : b) {
                if(jsonNode2.has("text")) {
                    String hashTagName = jsonNode2.get("text").toString();
                    String hashTagText = jsonNode.get("text").toString();
                    out.collect(new Tuple2<String, String>(hashTagName, hashTagText));
                }
            }
        }
    }
}
