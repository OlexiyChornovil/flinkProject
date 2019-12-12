package org.bdcourse.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterListsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.HashtagSelect;
import org.bdcourse.maps.SelectTweetHashtags;
import org.bdcourse.source.TwitterSourceDelivery;


public class TwitterStreamHashtagCount {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		TwitterStreamHashtagCount t = new TwitterStreamHashtagCount();
		StreamExecutionEnvironment env = t.getPipe();
		env.execute();
	}
	public StreamExecutionEnvironment getPipe() throws Exception {
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

		DataStream<Tuple2<String, Integer>> tweets = streamSource
				.filter(new TweetContainingHashtag())
				.flatMap(new SelectTweetHashtags())
				.filter(new FilterListsFromList())
				.flatMap(new HashtagSelect());
		tweets.writeAsText(jobParameters.get("TwitterStreamHashtagCountOutput"), WriteMode.OVERWRITE).setParallelism(1);
		tweets.print();
		return env;
	}

}

