package org.bdcourse;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.bdcourse.source.TwitterSourceDelivery;

import java.util.ArrayList;
import java.util.List;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.Projection;
//import com.google.gson.JsonArray;

public class TwitterStream {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
		TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		/*
		if (esOutput) {
			createElasticsearchMapping(indexName, typeName, deleteIndex);
		}
		*/

		DataStream<String> streamSource = env.addSource(twitterSource);
		DataStream<String> tweets = streamSource
				.filter(new TweetContainingHashtag())
				.flatMap(new Projection());


		tweets.writeAsText(jobParameters.get("output"), WriteMode.OVERWRITE).setParallelism(1);
		env.execute();
	}

}

