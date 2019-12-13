package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public class HashtagWordCount implements FlatMapFunction<Tuple2<List<String>, String>, Tuple2<Integer, Integer>> {
    @Override
    public void flatMap(Tuple2<List<String>, String> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
        Integer wordCount = value.f1.split(" ").length;
        Integer hashtagCount = value.f0.size();
        out.collect(new Tuple2<Integer, Integer>(wordCount, hashtagCount));
    }
}