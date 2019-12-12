package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class LongestWord implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {
    @Override
    public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> out) throws Exception {
        String[] lst = value.f1.split(" ");
        String tmp = "";
        for (String s : lst) {
            if (tmp.length()<s.length()){
                tmp=s;
            }
        }
        out.collect(new Tuple2<String, String>(value.f0, tmp));
    }
}
