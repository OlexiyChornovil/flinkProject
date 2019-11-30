package org.bdcourse;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import org.bdcourse.maps.SelectTweetsWithHashtags;
import org.bdcourse.maps.WordCount;
import org.bdcourse.filters.FilterTweetsFromList;

//import com.google.gson.JsonArray;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.TreeNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class TwitterStreamImplementation {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		
//		Arguments for twitter connection and location of output file
//      --twitter-source.consumerKey C5ED8IZeE8uf1pBYBAD6fVlBr
//		--twitter-source.consumerSecret vxCM8NfN2xJtmrYQe08TjhoMaKTBuN5HEIopZakjTx3dsD0ONf
//		--twitter-source.token 1049954129886531584-I9n2BnNmajY5W3CQ0TkA6lWVfm6P4j
//		--twitter-source.tokenSecret bOZnd54AnVUpaHxozQg5bXvEVBlFdaGzn3UYpVhZrm6Rb
//		--output /home/alex/Projects/output
		final ParameterTool params = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(params.getInt("parallelism", 1));

		DataStream<String> streamSource = null;

		if (params.has(TwitterSource.CONSUMER_KEY) &&
				params.has(TwitterSource.CONSUMER_SECRET) &&
				params.has(TwitterSource.TOKEN) &&
				params.has(TwitterSource.TOKEN_SECRET)
				) {
			streamSource = env.addSource(new TwitterSource(params.getProperties()));
			System.out.println("Twitter source initialized");
		} else {
			System.out.println("Executing TwitterStream example with default props.");
			System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
					"--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
			// get default test text data
			//streamSource = env.fromElements(TwitterExampleData.TEXTS);
		}

		/**
		DataStream<Tuple3<String, Double, Integer>> tweets = streamSource
	
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweets())
				.flatMap(new WordCount()).keyBy(0)
				.reduce(new ReduceFunction<Tuple3<String,Double,Integer>>() {
					
					@Override
					public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> value1,
							Tuple3<String, Double, Integer> value2) throws Exception {
						// TODO Auto-generated method stub
						String key = value1.f0;
						double everage = value1.f1;
						int count = value1.f2;
						int newCount = count + 1;
						double newEverage = ((count * everage) + value2.f1) / newCount;
						return new Tuple3<>(key, newEverage, newCount);
					}
				});
		*/


		DataStream<Tuple3<String, Double, Integer>> tweets = streamSource

				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweetsFromList())
				.flatMap(new WordCount()).keyBy(0);

		/*
		DataStream<String> tweets = streamSource
				.flatMap(new Projection());
		*/

		// emit result
		if (params.has("output")) {
			tweets.writeAsText(params.get("output"), WriteMode.OVERWRITE).setParallelism(1);
			tweets.print();

		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}

	public static class Transform implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>{
		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<>(value.f0, 1));	
		}
		
	}



	public static class Projection implements FlatMapFunction<String, String> {
		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			out.collect(value);
		}
	}
}

