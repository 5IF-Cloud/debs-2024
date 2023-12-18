package com.example.debs.operators;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>, Long, TimeWindow> {

    @Override
    public void process(Long key, Context context, Iterable<Tuple2<Long, Long>> nbFailByVault,
                        Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
        for (Tuple2<Long, Long> tuple : nbFailByVault) {
            collector.collect(new Tuple3<>(tuple.f0, tuple.f1, context.window().getEnd()));
        }
    }
}
