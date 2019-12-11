package org.bdcourse.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterListsFromList;
import org.bdcourse.filters.FilterTweetsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.HashtagSelect;
import org.bdcourse.maps.SelectTweetHashtags;
import org.bdcourse.maps.SelectTweetsWithHashtags;
import org.bdcourse.maps.WordCountGetNumbers;
import org.bdcourse.source.TwitterSourceDelivery;

import java.util.List;

public class TwitterStreamHashtagCount {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
		TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<String> streamSource = null;
		streamSource = env.addSource(twitterSource);

		DataStream<Tuple2<String, Integer>> tweets = streamSource
				.filter(new TweetContainingHashtag())
				.flatMap(new SelectTweetHashtags())
				.filter(new FilterListsFromList())
				.flatMap(new HashtagSelect());
		tweets.writeAsText(jobParameters.get("TwitterStreamHashtagCountOutput"), WriteMode.OVERWRITE).setParallelism(1);
		tweets.print();
		env.execute();
	}

}

