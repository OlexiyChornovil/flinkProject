package org.bdcourse.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class FilterTweetsWithRetweetsFromList implements FilterFunction<Tuple2<String, Integer>> {
    private List<String> list;

    public FilterTweetsWithRetweetsFromList() {
        list = new ArrayList<>();
        list.add("\"china\"");
        list.add("\"russia\"");
        list.add("\"usa\"");
        list.add("\"germany\"");
    }

    @Override
    public boolean filter(Tuple2<String, Integer> value) throws Exception {
        String s = value.f0.toLowerCase();
        for(String hash : list) {
            if(s.contains(hash)) {
                return true;
            }
        }
        return false;
    }
}
