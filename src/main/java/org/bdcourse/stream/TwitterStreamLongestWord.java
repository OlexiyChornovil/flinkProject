package org.bdcourse.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterTweetsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.LongestWord;
import org.bdcourse.maps.SelectTweetsWithHashtags;
import org.bdcourse.maps.WordCount;
import org.bdcourse.source.TwitterSourceDelivery;

public class TwitterStreamLongestWord {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
		TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<String> streamSource = null;
		streamSource = env.addSource(twitterSource);

		DataStream<Tuple2<String, String>> tweets = streamSource
				.filter(new TweetContainingHashtag())
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweetsFromList())
				.flatMap(new LongestWord());


		tweets.writeAsText(jobParameters.get("TwitterStreamWordCountOutput"), WriteMode.OVERWRITE).setParallelism(1);
		tweets.print();
		env.execute();
	}

}

