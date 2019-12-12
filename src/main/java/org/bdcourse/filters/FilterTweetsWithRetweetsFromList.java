package org.bdcourse.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.bdcourse.tools.TwitterHashtagsListCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterTweetsWithRetweetsFromList implements FilterFunction<Tuple2<String, Integer>> {
    private List<String> list;

    public FilterTweetsWithRetweetsFromList() throws IOException {
        TwitterHashtagsListCreator t = new TwitterHashtagsListCreator();
        list = t.getList();
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
