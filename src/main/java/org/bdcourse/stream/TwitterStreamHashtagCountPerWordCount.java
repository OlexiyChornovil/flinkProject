package org.bdcourse.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterListsFromList;
import org.bdcourse.filters.FilterListsFromListForRegression;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.HashtagSelect;
import org.bdcourse.maps.HashtagWordCount;
import org.bdcourse.maps.SelectTweetHashtags;
import org.bdcourse.maps.SelectTweetTextWithHashtagList;
import org.bdcourse.source.TwitterSourceDelivery;


public class TwitterStreamHashtagCountPerWordCount {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		TwitterStreamHashtagCountPerWordCount t = new TwitterStreamHashtagCountPerWordCount();
		StreamExecutionEnvironment env = t.getPipe();
		env.execute();
	}
	public StreamExecutionEnvironment getPipe() throws Exception {
		ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");

		DataStream<String> streamSource = null;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		if(jobParameters.get("debug").equals("true")){
			System.out.println("DEBUG ON");
			streamSource = env.readTextFile(jobParameters.get("TwitterBatchHashtagCoutPerWordCountInput"));
		}
		else{
			TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
			env.setParallelism(1);
			streamSource = env.addSource(twitterSource);
		}

		DataStream<Tuple2<Integer, Integer>> tweets = streamSource
				.flatMap(new SelectTweetTextWithHashtagList())
				.filter(new FilterListsFromListForRegression())
				.flatMap(new HashtagWordCount());

		tweets.writeAsText(jobParameters.get("TwitterStreamHashtagCountPerWordCountOutput"), WriteMode.OVERWRITE).setParallelism(1);
		tweets.print();
		return env;
	}

}

