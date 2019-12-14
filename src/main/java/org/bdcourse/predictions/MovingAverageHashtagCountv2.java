package org.bdcourse.predictions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterListsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.HashtagSelect;
import org.bdcourse.maps.SelectTweetHashtags;
import org.bdcourse.process.MovingAverageProcess;
import org.bdcourse.process.MovingAverageProcessv2;
import org.bdcourse.source.TwitterSourceDelivery;

import java.util.List;

public class MovingAverageHashtagCountv2 {
    public static void main(String[] args) throws Exception {
        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        List<Tuple2<String, Integer>> batchData = getBatchResults(jobParameters);

        Integer amount = batchData.size();
        Integer sum = 0;
        for (Tuple2<String, Integer> item:batchData){
            sum+=item.f1;
        }

        DataStream<String> streamSource = null;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if(jobParameters.get("debug").equals("true")){
            System.out.println("DEBUG ON");
            streamSource = env.readTextFile(jobParameters.get("RegressionHashtagCountPerWordCountStreamInput"));
        }
        else{
            TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
            env.setParallelism(1);
            streamSource = env.addSource(twitterSource);
        }

        DataStream<Tuple3<String, Integer, Double>> stream = streamSource
                .filter(new TweetContainingHashtag())
                .flatMap(new SelectTweetHashtags())
                .filter(new FilterListsFromList())
                .flatMap(new HashtagSelect())
                .keyBy(0)
                //.process(new MovingAverageProcessv2());
                .process(new MovingAverageProcessv2(amount, sum));

        stream.writeAsText(jobParameters.get("MovingAverageHashtagCountOutput"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        stream.print();
        env.execute();

    }


    private static List<Tuple2<String, Integer>> getBatchResults(ParameterTool jobParameters) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> tweetText = env.readTextFile(jobParameters.get("MovingAverageWordCountInput"));

        DataSet<Tuple2<String, Integer>> tweets = tweetText
                .filter(new TweetContainingHashtag())
                .flatMap(new SelectTweetHashtags())
                .filter(new FilterListsFromList())
                .flatMap(new HashtagSelect());
        return tweets.collect();
    }
}
