package org.bdcourse.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.bdcourse.filters.FilterFavorited;
import org.bdcourse.filters.FilterListsFromListForRegression;
import org.bdcourse.filters.FilterTweetsWithRetweetsFromList;
import org.bdcourse.maps.HashtagWordCount;
import org.bdcourse.maps.SelectHashtagWithLikeCount;
import org.bdcourse.maps.SelectTweetTextWithHashtagList;

public class TwitterBatchHashtagCoutPerWordCount {
    public static void main(String[] args) throws Exception{
        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> tweetText = env.readTextFile(jobParameters.get("TwitterBatchLikeCountInput"));

        DataSet<Tuple2<Integer, Integer>> tweets = tweetText
                .flatMap(new SelectTweetTextWithHashtagList())
                .filter(new FilterListsFromListForRegression())
                .flatMap(new HashtagWordCount());

        tweets.writeAsText(jobParameters.get("TwitterBatchHashtagCountPerWordCountOutput"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        tweets.print();
        //env.execute();
    }
}
