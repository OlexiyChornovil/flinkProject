package org.bdcourse.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class TweetContainingHashtag  implements FilterFunction<String> {

    private transient ObjectMapper jsonParser;
    @Override
    public boolean filter(String value) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        boolean hasHashtags = jsonNode.has("entities") && jsonNode.get("entities").has("hashtags");
        if(hasHashtags) {
            JsonNode tmp = jsonNode.get("entities").get("hashtags");
            for (JsonNode jsonNode2 : tmp) {
                if(jsonNode2.has("text")) {
                    return true;
                }
            }
        }
        return false;
    }
}
