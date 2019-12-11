package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.xml.Null;

import java.util.ArrayList;
import java.util.List;

public class HashtagSelect implements FlatMapFunction<List<String>, Tuple2<String, Integer>> {
    private List<String> list;

    public HashtagSelect() {
        list = new ArrayList<>();
        list.add("\"china\"");
        list.add("\"russia\"");
        list.add("\"usa\"");
        list.add("\"germany\"");
    }

    @Override
    public void flatMap(List<String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String main_hash = null;
        for(String hash : list) {
            for(String s: value) {
                if(s.toLowerCase().contains(hash)){
                    main_hash = hash;
                }
            }
        }
        out.collect(new Tuple2<String, Integer>(main_hash, list.size()));
    }
}