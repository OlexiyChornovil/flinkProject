package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class SelectHashtagWithLikeCount implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private transient ObjectMapper jsonParser;
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
        if(hasHashtags) {
            JsonNode tmp = jsonNode.get("entities").get("hashtags");
            for (JsonNode jsonNode2 : tmp) {
                if(jsonNode2.has("text")) {
                    String hashTagName = jsonNode2.get("text").toString();
                    Integer replyCount = jsonNode.get("favorite_count").intValue();
                    out.collect(new Tuple2<String, Integer>(hashTagName, replyCount));

                }
            }
        }
    }

}
