package org.bdcourse.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MovingAverageProcessv2 extends ProcessFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Double>> {
    Integer amount=0;
    Integer sum=0;

    private ValueState<Integer> currentState;


    public MovingAverageProcessv2(Integer amount, Integer sum){
        this.amount = amount;
        this.sum = sum;
    }


    @Override
    public void open(Configuration conf) throws Exception{
        currentState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("Mystate", Integer.class));
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context context, Collector<Tuple3<String, Integer, Double>> out) throws Exception {

        Double average = ((double)sum)/((double)amount);
        Integer intAverage = average.intValue();


        amount+=1;
        sum+= value.f1;
        out.collect(new Tuple3<String, Integer, Double>(value.f0, value.f1, average));
    }

}
