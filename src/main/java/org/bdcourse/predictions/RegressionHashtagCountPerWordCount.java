package org.bdcourse.predictions;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.bdcourse.filters.FilterListsFromListForRegression;
import org.bdcourse.maps.HashtagWordCount;
import org.bdcourse.maps.SelectTweetTextWithHashtagList;
import org.bdcourse.maps.SelectTweetsWithHashtags;
import org.bdcourse.source.TwitterSourceDelivery;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RegressionHashtagCountPerWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        List<Tuple2<Integer, Integer>> batchData = getBatchResults(jobParameters);
        SimpleRegression regression = new SimpleRegression();

        double[][] data = new double[batchData.size()][2];
        for(int i = 0; i<batchData.size(); i++) {
            data[i][0] = (double)batchData.get(i).f0;
            data[i][1] = (double)batchData.get(i).f1;
        }
        //System.out.println(Arrays.deepToString(data));

        regression.addData(data);
        //regression data from the batch-computation


        //System.out.println(intercept);
        //System.out.println(slope);

        DataStream<String> streamSource = null;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if(jobParameters.get("debug").equals("true")){
            System.out.println("DEBUG ON");
            streamSource = env.readTextFile(jobParameters.get("RegressionHashtagCountPerWordCountStreamInput"));
        }
        else{
            TwitterSource twitterSource = TwitterSourceDelivery.getTwitterConnection();
            env.setParallelism(1);
            streamSource = env.addSource(twitterSource);
        }

        DataStream<Tuple4<Integer, Integer, Integer, Double>> stream = streamSource
                .flatMap(new SelectTweetTextWithHashtagList())
                .filter(new FilterListsFromListForRegression())
                .flatMap(new HashtagWordCount())

                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple4<Integer, Integer, Integer, Double>>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context context, Collector<Tuple4<Integer, Integer, Integer, Double>> out) throws Exception {
                        Double intercept = regression.getIntercept();
                        Double slope = regression.getSlope();
                        Double tmp = (intercept + slope * value.f0);
                        Integer predictionpoint = tmp.intValue();
                        double[] d = new double[1];
                        d[0] = value.f0;
                        regression.addObservation(d, value.f1);
                        out.collect(new Tuple4<Integer, Integer, Integer, Double>(value.f0, value.f1, predictionpoint, tmp));
                    }
                });
        stream.writeAsText(jobParameters.get("RegressionHashtagCountPerWordCountOutput"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        stream.print();
        env.execute();

    }


    private static List<Tuple2<Integer, Integer>> getBatchResults(ParameterTool jobParameters) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> tweetText = env.readTextFile(jobParameters.get("RegressionHashtagCountPerWordCountBatchInput"));

        DataSet<Tuple2<Integer, Integer>> tweets = tweetText
                .flatMap(new SelectTweetTextWithHashtagList())
                .filter(new FilterListsFromListForRegression())
                .flatMap(new HashtagWordCount());
        return tweets.collect();
    }
}
