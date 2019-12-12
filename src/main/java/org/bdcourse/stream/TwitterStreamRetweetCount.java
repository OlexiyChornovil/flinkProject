package org.bdcourse.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterRetweets;
import org.bdcourse.filters.FilterTweetsFromList;
import org.bdcourse.filters.FilterTweetsWithRetweetsFromList;
import org.bdcourse.maps.SelectHashtagWithRetweetCount;
import org.bdcourse.maps.SelectTweetsWithHashtags;
import org.bdcourse.source.TwitterSourceDelivery;

public class TwitterStreamRetweetCount {
    public static void main(String[] args) throws Exception{
        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");

        DataStream<String> streamSource = null;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if(jobParameters.get("debug").equals("true")){
            System.out.println("DEBUG ON");
            streamSource = env.readTextFile(jobParameters.get("TwitterBatchLikeCountInput"));
        }
        else{
            TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
            env.setParallelism(1);
            streamSource = env.addSource(twitterSource);
        }


        DataStream<Tuple2<String, Integer>>tweets = streamSource
                .filter(new FilterRetweets())
                .flatMap(new SelectHashtagWithRetweetCount())
                .filter(new FilterTweetsWithRetweetsFromList());

        tweets.writeAsText(jobParameters.get("TwitterStreamRetweetCountOutput"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        tweets.print();
        env.execute();
    }
}
