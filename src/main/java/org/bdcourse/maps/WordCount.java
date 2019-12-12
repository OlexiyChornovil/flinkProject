package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class WordCount implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
    @Override
    public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer count = value.f1.split(" ").length;
        out.collect(new Tuple2<String, Integer>(value.f0, count));
    }
}
