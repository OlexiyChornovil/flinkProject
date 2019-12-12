package org.bdcourse.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.bdcourse.filters.FilterTweetsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.SelectTweetsWithHashtags;
import org.bdcourse.maps.WordCount;
import org.bdcourse.source.TwitterSourceDelivery;

public class TwitterBatchWordCount {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> tweetText = env.readTextFile(jobParameters.get("TwitterBatchWordCountInput"));

		DataSet<Tuple2<String, Double>> tweets = tweetText
				.filter(new TweetContainingHashtag())
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweetsFromList())
				.flatMap(new WordCount());


		tweets.writeAsText(jobParameters.get("TwitterStreamWordCountOutput"), WriteMode.OVERWRITE).setParallelism(1);
		tweets.print();
		env.execute();
	}

}

