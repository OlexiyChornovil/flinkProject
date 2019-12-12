package org.bdcourse.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.bdcourse.tools.TwitterHashtagsListCreator;
import scala.xml.Null;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HashtagSelect implements FlatMapFunction<List<String>, Tuple2<String, Integer>> {
    private List<String> list;

    public HashtagSelect() throws IOException {
        TwitterHashtagsListCreator t = new TwitterHashtagsListCreator();
        list = t.getList();
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
        Tuple2<String, Integer> output = new Tuple2<String, Integer>(main_hash, value.size());
        /*
        System.out.println("------");
        System.out.println(output);
        System.out.println("------");
         */

        out.collect(output);
    }
}