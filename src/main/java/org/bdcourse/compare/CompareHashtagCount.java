package org.bdcourse.compare;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bdcourse.filters.FilterListsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.HashtagSelect;
import org.bdcourse.maps.SelectTweetHashtags;

import java.io.IOException;
import java.util.List;

public class CompareHashtagCount {
    public static void main(String[] args) throws Exception {

        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        List<Tuple2<String, Integer>> batch = batchProcess(jobParameters);

    }

    private static List<Tuple2<String, Integer>> batchProcess(ParameterTool jobParameters) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> tweetText = env.readTextFile(jobParameters.get("TwitterBatchHashtagCountInput"));
        List<Tuple2<String, Integer>> tweets = tweetText
                .filter(new TweetContainingHashtag())
                .flatMap(new SelectTweetHashtags())
                .filter(new FilterListsFromList())
                .flatMap(new HashtagSelect())
                .collect();
        return tweets;
    }
}
