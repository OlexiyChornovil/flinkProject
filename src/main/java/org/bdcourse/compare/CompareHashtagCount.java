package org.bdcourse.compare;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.bdcourse.filters.FilterListsFromList;
import org.bdcourse.filters.TweetContainingHashtag;
import org.bdcourse.maps.HashtagSelect;
import org.bdcourse.maps.SelectTweetHashtags;
import org.bdcourse.source.TwitterSourceDelivery;

import java.io.IOException;
import java.util.List;

public class CompareHashtagCount {
    public static void main(String[] args) throws Exception {

        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        DataStream<Tuple2<String, Integer>> batch = batchProcess(jobParameters);

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

        DataStream<Tuple2<String, Integer>> stream = streamSource
                .filter(new TweetContainingHashtag())
                .flatMap(new SelectTweetHashtags())
                .filter(new FilterListsFromList())
                .flatMap(new HashtagSelect());

        DataStream<Tuple2<String, Integer>> finalstream =  stream.connect(batch)
                .keyBy(0, 0)
                .flatMap(new RichCoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private Integer batchValue=0;

                    @Override
                    public void flatMap2(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        batchValue = stringIntegerTuple2.f1;


                    }

                    @Override
                    public void flatMap1(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple2<String, Integer>> collector) throws Exception {

                            Tuple2<String, Integer> output = new Tuple2<String, Integer>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 - batchValue);
                            collector.collect(output);
                    }
                });
        finalstream.print();
        finalstream.writeAsText(jobParameters.get("CompareHashtagCountOutput"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

    }

    private static DataStream<Tuple2<String, Integer>> batchProcess(ParameterTool jobParameters) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> tweetText = env.readTextFile(jobParameters.get("TwitterBatchHashtagCountInput"));
        DataStream<Tuple2<String, Integer>> tweets = tweetText
                .filter(new TweetContainingHashtag())
                .flatMap(new SelectTweetHashtags())
                .filter(new FilterListsFromList())
                .flatMap(new HashtagSelect())

                .flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer, Integer>> out)
                            throws Exception {
                            out.collect(new Tuple3<String, Integer, Integer>(value.f0, value.f1, 1));
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2)
                            throws Exception {
                        return new Tuple3<String, Integer, Integer>(value1.f0, value1.f1 + value2.f1, value1.f2+value2.f2);
                    }
                })
                .flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, Integer>> out)
                            throws Exception {
                        out.collect(new Tuple2<String, Integer>(value.f0, (int)(value.f1/value.f2)));
                    }
                });
        return tweets;
    }



}
