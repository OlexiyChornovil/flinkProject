package org.bdcourse.predictions;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.bdcourse.source.TwitterSourceDelivery;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RegressionHashtagCountPerWordCountv3 {
    public static void main(String[] args) throws Exception {
        ParameterTool jobParameters = ParameterTool.fromPropertiesFile("src/main/resources/JobConfig.properties");
        List<Tuple2<Integer, Integer>> batchData = readBatch(
                "./data/TwitterBatchHashtagCountPerWordCountOutput");
        SimpleRegression regression = new SimpleRegression();

        double[][] data = new double[batchData.size()][2];
        for(int i = 0; i<batchData.size(); i++) {
            data[i][0] = (double)batchData.get(i).f0;
            data[i][1] = (double)batchData.get(i).f1;
        }
        regression.addData(data);

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
                .keyBy(0)
                .process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple4<Integer, Integer, Integer, Double>>() {
                    private ValueState<SimpleRegression> currentState;


                    @Override
                    public void open(Configuration conf){
                        currentState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("Mystate", SimpleRegression.class));
                    }

                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context context, Collector<Tuple4<Integer, Integer, Integer, Double>> out) throws Exception {

                        if (currentState.value() == null){
                            currentState.update(regression);
                        }
                        Double inter = currentState.value().getIntercept();
                        Double sl = currentState.value().getSlope();

                        Double tmp = (inter + sl * value.f0);
                        Integer predictionpoint = tmp.intValue();
                        double[] d = new double[1];
                        d[0] = value.f0;
                        SimpleRegression r = currentState.value();
                        r.addObservation(d, value.f1);
                        currentState.update(r);
                        //regression.addObservation(d, value.f1);
                        out.collect(new Tuple4<Integer, Integer, Integer, Double>(value.f0, value.f1, predictionpoint, tmp));
                    }
                });
        stream.writeAsText(jobParameters.get("RegressionHashtagCountPerWordCountOutput"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        stream.print();
        env.execute();

    }

    public static List<Tuple2<Integer, Integer>> readBatch(String path) throws Exception {
        BufferedReader reader;
        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        try {
            reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                line = line.replace("(", "");
                line = line.replace(")", "");
                String[] parts = line.split(",");
                Tuple2<Integer, Integer> t = new Tuple2<Integer, Integer>(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                list.add(t);
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}
