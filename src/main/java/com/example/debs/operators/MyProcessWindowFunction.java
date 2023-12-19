package com.example.debs.operators;

import com.example.debs.model.InputMessage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction<InputMessage, Tuple3<Long, Long, Long>, Long, TimeWindow> {

    @Override
    public void process(Long key, Context context, Iterable<InputMessage> inputMessages, Collector<Tuple3<Long, Long, Long>> output) {
        long count = 0;
        for (InputMessage inputMessage : inputMessages) {
            count++;
        }
        output.collect(new Tuple3<>(key, context.window().getStart(), count));
    }
}
