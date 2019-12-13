package org.bdcourse.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.bdcourse.tools.TwitterHashtagsListCreator;

import java.io.IOException;
import java.util.List;

public class FilterListsFromListForRegression implements FilterFunction<Tuple2<List<String>, String>> {
    private List<String> list;

    public FilterListsFromListForRegression() throws IOException {
        TwitterHashtagsListCreator t = new TwitterHashtagsListCreator();
        list = t.getList();
    }

    @Override
    public boolean filter(Tuple2<List<String>, String> value) throws Exception {
        String main_hash = null;
        for(String hash : list) {
            for(String s: value.f0) {
                if(s.toLowerCase().contains(hash)){
                    return true;
                }
            }
        }
        return false;
    }
}
