package org.bdcourse.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class FilterListsFromList implements FilterFunction<List<String>> {
    private List<String> list;

    public FilterListsFromList() {
        list = new ArrayList<>();
        list.add("\"china\"");
        list.add("\"blackfriday\"");
    }

    @Override
    public boolean filter(List<String> value) throws Exception {
        String main_hash = null;
        for(String hash : list) {
            for(String s: value) {
                if(s.contains(hash)){
                    return true;
                }
            }
        }
        return false;
    }
}
