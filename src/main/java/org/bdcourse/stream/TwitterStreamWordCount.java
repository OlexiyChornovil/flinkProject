package org.bdcourse.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.bdcourse.filters.FilterTweetsFromList;
import org.bdcourse.maps.WordCount;
import org.bdcourse.maps.WordCountGetNumbers;
import org.bdcourse.source.TwitterSourceDelivery;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.Projection;
import org.bdcourse.maps.SelectTweetsWithHashtags;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class TwitterStreamWordCount {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
		TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<String> streamSource = null;
		streamSource = env.addSource(twitterSource);

		DataStream<Tuple2<String, Double>> tweets = streamSource
				.filter(new TweetContainingHashtag())
				.flatMap(new SelectTweetsWithHashtags())
				.filter(new FilterTweetsFromList())
				.flatMap(new WordCount());

		/*
		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
		httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"));

		// use a ElasticsearchSink.Builder to create an ElasticsearchSink
		ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
		httpHosts,
		new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) {
				Map<String, String> json = new HashMap<>();
				json.put("data", element);

				return Requests.indexRequest()
						.index("my-index")
						.type("my-type")
						.source(json);
			}

			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				indexer.add(createIndexRequest(element));
			}
		});
		tweets.addSink(esSinkBuilder.build());

		 */



		tweets.writeAsText(jobParameters.get("output"), WriteMode.OVERWRITE).setParallelism(1);
		tweets.print();
		env.execute();
	}

}

