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
        list.add("\"russia\"");
        list.add("\"usa\"");
        list.add("\"germany\"");
    }

    @Override
    public boolean filter(List<String> value) throws Exception {
        String main_hash = null;
        for(String hash : list) {
            for(String s: value) {
                if(s.toLowerCase().contains(hash)){
                    return true;
                }
            }
        }
        return false;
    }
}
