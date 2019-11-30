package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class WordCount implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, Double, Integer>> {
    @Override
    public void flatMap(Tuple2<String, String> value, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
        double count = value.f1.split(" ").length;
        out.collect(new Tuple3<String, Double, Integer>(value.f0, count, 1));
    }
}
