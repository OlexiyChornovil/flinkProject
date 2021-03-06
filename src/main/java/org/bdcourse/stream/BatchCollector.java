package org.bdcourse.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterFavorited;
import org.bdcourse.filters.FilterTweetsWithRetweetsFromList;
import org.bdcourse.maps.Projection;
import org.bdcourse.maps.SelectHashtagWithLikeCount;
import org.bdcourse.source.TwitterSourceDelivery;

public class BatchCollector {
    public static void main(String[] args) throws Exception {
        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> streamSource = null;
        streamSource = env.addSource(twitterSource);

        DataStream<String> tweets = streamSource
                .flatMap(new Projection());

        tweets.writeAsText(jobParameters.get("BatchCollectorOutput"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        tweets.print();
        env.execute();
    }
}