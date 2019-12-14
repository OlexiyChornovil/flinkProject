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

    private ValueState<Integer> amountState;
    private ValueState<Integer> sumState;


    public MovingAverageProcessv2(Integer amount, Integer sum){
        this.amount = amount;
        this.sum = sum;
    }


    @Override
    public void open(Configuration conf) throws Exception{
        amountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("Mystate", Integer.class));
        sumState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("Mystate2", Integer.class));

    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context context, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
        if (amountState.value() == null){
            amountState.update(this.amount);
            sumState.update(this.sum);
        }
        Double average = ((double)sumState.value())/((double)amountState.value());
        Integer intAverage = average.intValue();

        Integer tmp1 = amountState.value() + 1;
        amountState.update(tmp1);
        Integer tmp2 = sumState.value() + value.f1;
        sumState.update(tmp2);
        out.collect(new Tuple3<String, Integer, Double>(value.f0, value.f1, average));
    }

}
