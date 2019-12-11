package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class WordCountGetNumbers implements FlatMapFunction<Tuple2<String, String>, String> {
    @Override
    public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
        double count = value.f1.split(" ").length;
        out.collect(new String(value.f0));
    }
}
