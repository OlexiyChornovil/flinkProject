package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class SelectTweetTextWithHashtagList implements FlatMapFunction<String, Tuple2<List<String>, String>> {
    private transient ObjectMapper jsonParser;
    @Override
    public void flatMap(String value, Collector<Tuple2<List<String>, String>> out) throws Exception {

        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
        if(hasHashtags) {
            JsonNode tmp = jsonNode.get("entities").get("hashtags");
            List<String> hashtagList = new ArrayList<>();
            for (JsonNode jsonNode2 : tmp) {
                if(jsonNode2.has("text")) {
                    hashtagList.add(jsonNode2.get("text").toString());
                }
            }
            String hashTagText = jsonNode.get("text").toString();
            out.collect(new Tuple2<List<String>, String>(hashtagList, hashTagText));

        }
    }
}
