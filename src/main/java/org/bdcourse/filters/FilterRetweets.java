package org.bdcourse.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class FilterRetweets implements FilterFunction<String> {
    private transient ObjectMapper jsonParser;

    @Override
    public boolean filter(String value) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        return jsonNode.has("retweeted_status");
    }
}
