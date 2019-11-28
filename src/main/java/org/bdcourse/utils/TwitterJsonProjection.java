package org.bdcourse.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class TwitterJsonProjection  implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        out.collect(value);
    }
}

