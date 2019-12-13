package org.bdcourse.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.bdcourse.tools.TwitterHashtagsListCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterListsFromList implements FilterFunction<List<String>> {
    private List<String> list;

    public FilterListsFromList() throws IOException {
        TwitterHashtagsListCreator t = new TwitterHashtagsListCreator();
        list = t.getList();
    }

    @Override
    public boolean filter(List<String> value) throws Exception {
        String main_hash = null;
        System.out.println(value);
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
