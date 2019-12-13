package org.bdcourse.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterListsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.HashtagSelect;
import org.bdcourse.maps.SelectTweetHashtags;
import org.apache.flink.util.Collector;
import org.bdcourse.source.TwitterSourceDelivery;

import java.io.IOException;
import java.util.Map;

public class TwitterBatchHashtagCount {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		TwitterBatchHashtagCount t = new TwitterBatchHashtagCount();
		ExecutionEnvironment env = t.getPipe();
		//env.execute();
	}

	public ExecutionEnvironment getPipe() throws Exception {

		ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> tweetText = env.readTextFile(jobParameters.get("TwitterBatchHashtagCountInput"));


		DataSet<Tuple2<String, Integer>> tweets = tweetText
				.filter(new TweetContainingHashtag())
				.flatMap(new SelectTweetHashtags())
				.filter(new FilterListsFromList())
				.flatMap(new HashtagSelect())
				;

		tweets.writeAsText(jobParameters.get("TwitterStreamHashtagCountOutput"), WriteMode.OVERWRITE).setParallelism(1);
		tweets.print();
		return env;
	}

}

