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

//import com.google.gson.JsonArray;

public class TwitterStreamTest {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		
//		Arguments for twitter connection and location of output file
//      --twitter-source.consumerKey C5ED8IZeE8uf1pBYBAD6fVlBr
//		--twitter-source.consumerSecret vxCM8NfN2xJtmrYQe08TjhoMaKTBuN5HEIopZakjTx3dsD0ONf
//		--twitter-source.token 1049954129886531584-I9n2BnNmajY5W3CQ0TkA6lWVfm6P4j
//		--twitter-source.tokenSecret bOZnd54AnVUpaHxozQg5bXvEVBlFdaGzn3UYpVhZrm6Rb
//		--output /home/alex/Projects/output
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
				.flatMap(new Projection());

		tweets.writeAsText(jobParameters.get("output"), WriteMode.OVERWRITE).setParallelism(1);
		env.execute();
	}

	public static class Projection implements FlatMapFunction<String, String> {
		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			System.out.println(value);
			out.collect(value);
		}
	}
}

