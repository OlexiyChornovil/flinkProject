package org.bdcourse.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MovingAverageProcessv2 extends ProcessFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Double>> {
    Integer amount;
    Integer sum;
    private ValueState<Integer> currentState;

    public MovingAverageProcessv2(Integer amount, Integer sum){
        this.amount = amount;
        this.sum = sum;
    }
    @Override
    public void open(Configuration conf){
        currentState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("state", Integer.class));
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context context, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
        Double average = ((double)this.sum)/((double)this.amount);
        Integer intAverage = average.intValue();
        this.amount+=1;
        this.sum+= value.f1;
        out.collect(new Tuple3<String, Integer, Double>(value.f0, value.f1, average));
    }
}
